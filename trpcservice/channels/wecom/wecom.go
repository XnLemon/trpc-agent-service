// Package wecom implements the text-only HTTPS callback for one verified
// WeCom self-built application Binding.
package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- WeCom requires SHA-1 callback signatures.
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

var (
	// ErrInvalid reports malformed callback configuration or payloads.
	ErrInvalid = errors.New("invalid wecom callback")
	// ErrVerification reports an untrusted WeCom callback without exposing detail.
	ErrVerification = errors.New("wecom callback verification failed")
)

const wecomBlockSize = 32

// Config contains one Binding's runtime-only callback credentials and trusted
// routing target. Values must be resolved outside persisted Binding state.
type Config struct {
	Token            string
	EncodingAESKey   string
	ReceiveID        string
	AgentID          string
	RouteKey         string
	Target           channels.RoutingTarget
	Dispatcher       gateway.DispatchService
	MaxBodyBytes     int64
	ExecutionTimeout time.Duration
}

// Handler owns no listener or goroutine; the process HTTP server owns both.
type Handler struct {
	token, receiveID, agentID, routeKey string
	key                                 []byte
	principal                           gateway.Principal
	dispatcher                          gateway.DispatchService
	maxBodyBytes                        int64
	executionTimeout                    time.Duration
}

// New validates a text callback Handler. The complete trusted target is never
// recovered from XML, query, or headers.
func New(config Config) (*Handler, error) {
	if strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.ReceiveID) == "" || strings.TrimSpace(config.AgentID) == "" || config.Dispatcher == nil {
		return nil, ErrInvalid
	}
	if err := config.Target.Validate(); err != nil || config.Target.Channel != channels.ChannelWeCom {
		return nil, ErrInvalid
	}
	principal, err := gateway.NewChannelPrincipal(config.Target)
	if err != nil {
		return nil, ErrInvalid
	}
	key, err := decodeAESKey(config.EncodingAESKey)
	if err != nil {
		return nil, ErrInvalid
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = 1 << 20
	}
	if config.MaxBodyBytes < 1 {
		return nil, ErrInvalid
	}
	if config.ExecutionTimeout == 0 {
		config.ExecutionTimeout = 4 * time.Minute
	}
	if config.ExecutionTimeout < 1 {
		return nil, ErrInvalid
	}
	return &Handler{token: config.Token, receiveID: config.ReceiveID, agentID: strings.TrimSpace(config.AgentID), routeKey: strings.Trim(config.RouteKey, "/"), key: key, principal: principal, dispatcher: config.Dispatcher, maxBodyBytes: config.MaxBodyBytes, executionTimeout: config.ExecutionTimeout}, nil
}

// ServeHTTP verifies the URL challenge or accepts one encrypted text message.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || r == nil {
		http.NotFound(w, r)
		return
	}
	if h.routeKey != "" {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 || parts[len(parts)-1] != h.routeKey {
			http.NotFound(w, r)
			return
		}
	}
	switch r.Method {
	case http.MethodGet:
		h.handleChallenge(w, r)
	case http.MethodPost:
		h.handleMessage(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleChallenge(w http.ResponseWriter, r *http.Request) {
	ciphertext := r.URL.Query().Get("echostr")
	if !h.validSignature(r.URL.Query().Get("msg_signature"), r.URL.Query().Get("timestamp"), r.URL.Query().Get("nonce"), ciphertext) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	plain, err := h.decrypt(ciphertext)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, string(plain))
}

func (h *Handler) handleMessage(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	decoder := xml.NewDecoder(io.LimitReader(r.Body, h.maxBodyBytes))
	var envelope callbackEnvelope
	if err := decoder.Decode(&envelope); err != nil || envelope.Encrypt == "" || !h.validSignature(r.URL.Query().Get("msg_signature"), r.URL.Query().Get("timestamp"), r.URL.Query().Get("nonce"), envelope.Encrypt) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var trailing callbackEnvelope
	if err := decoder.Decode(&trailing); err != io.EOF {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	plain, err := h.decrypt(envelope.Encrypt)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var message inboundXML
	if err := xml.Unmarshal(plain, &message); err != nil || message.MsgType != "text" || strings.TrimSpace(message.Content) == "" || strings.TrimSpace(message.MsgID) == "" || strings.TrimSpace(message.FromUserName) == "" || strings.TrimSpace(message.AgentID) != h.agentID {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	executionCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), h.executionTimeout)
	defer cancel()
	accepted := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		stream, err := h.dispatcher.Dispatch(executionCtx, gateway.DispatchRequest{Accepted: accepted, Principal: h.principal, Message: gateway.InboundMessage{Content: message.Content, ContentType: gateway.ContentTypeText, ExternalMessageID: message.MsgID, ExternalUserID: message.FromUserName, ConversationKind: channels.ConversationDirect, ExternalPeerID: message.FromUserName}})
		if err == nil && stream != nil {
			for range stream {
			}
		}
		result <- err
	}()
	select {
	case <-accepted:
		h.writeSuccess(w)
	case err := <-result:
		if errors.Is(err, gateway.ErrDuplicateMessage) {
			h.writeSuccess(w)
			return
		}
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	case <-r.Context().Done():
		return
	}
}

func (h *Handler) writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "success")
}

func (h *Handler) validSignature(signature, timestamp, nonce, ciphertext string) bool {
	if signature == "" || timestamp == "" || nonce == "" || ciphertext == "" {
		return false
	}
	parts := []string{h.token, timestamp, nonce, ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- required by the WeCom protocol.
	want := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(signature), []byte(want)) == 1
}

func decodeAESKey(value string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(value + "=")
	if err != nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	return key, nil
}

func (h *Handler) decrypt(value string) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, ErrVerification
	}
	block, err := aes.NewCipher(h.key)
	if err != nil {
		return nil, ErrVerification
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, h.key[:aes.BlockSize]).CryptBlocks(plain, ciphertext)
	plain, err = unpad(plain)
	if err != nil || len(plain) < 20 {
		return nil, ErrVerification
	}
	size := int(binary.BigEndian.Uint32(plain[16:20]))
	if size < 0 || size > len(plain)-20 {
		return nil, ErrVerification
	}
	if subtle.ConstantTimeCompare(plain[20+size:], []byte(h.receiveID)) != 1 {
		return nil, ErrVerification
	}
	return append([]byte(nil), plain[20:20+size]...), nil
}

func unpad(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, ErrVerification
	}
	count := int(value[len(value)-1])
	if count < 1 || count > wecomBlockSize || count > len(value) {
		return nil, ErrVerification
	}
	if !bytes.Equal(value[len(value)-count:], bytes.Repeat([]byte{byte(count)}, count)) {
		return nil, ErrVerification
	}
	return value[:len(value)-count], nil
}

type callbackEnvelope struct {
	Encrypt string `xml:"Encrypt"`
}
type inboundXML struct {
	MsgID        string `xml:"MsgId"`
	FromUserName string `xml:"FromUserName"`
	MsgType      string `xml:"MsgType"`
	AgentID      string `xml:"AgentID"`
	Content      string `xml:"Content"`
}

var _ http.Handler = (*Handler)(nil)
