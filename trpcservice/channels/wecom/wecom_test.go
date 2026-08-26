package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestDecodeAESKeyAndDecryptRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	plain := append([]byte(strings.Repeat("x", 16)), []byte{0, 0, 0, 3}...)
	plain = append(plain, []byte("abcRID")...)
	block, _ := aes.NewCipher(key)
	padded := append([]byte(nil), plain...)
	n := aes.BlockSize - len(padded)%aes.BlockSize
	padded = append(padded, bytes.Repeat([]byte{byte(n)}, n)...)
	encrypted := make([]byte, len(padded))
	iv := key[:aes.BlockSize]
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	h := &Handler{key: key, receiveID: "RID"}
	got, err := h.decrypt(base64.StdEncoding.EncodeToString(encrypted))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(plain[20:23]) {
		t.Fatalf("got %q", got)
	}
}

func TestSignatureAndInvalidPadding(t *testing.T) {
	h := &Handler{token: "token"}
	if h.validSignature("", "1", "2", "3") {
		t.Fatal("empty signature accepted")
	}
	if h.validSignature("bad", "1", "2", "3") {
		t.Fatal("bad signature accepted")
	}
	if _, err := unpad([]byte{1, 2}); err == nil {
		t.Fatal("invalid padding accepted")
	}
}

func TestProviderCachesTokenAndDeliversText(t *testing.T) {
	var tokenCalls, sendCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/cgi-bin/gettoken" {
			tokenCalls++
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"secret-token","expires_in":3600}`)
			return
		}
		if r.URL.Path == "/cgi-bin/message/send" {
			sendCalls++
			if r.URL.Query().Get("access_token") != "secret-token" {
				t.Errorf("token missing")
			}
			_, _ = io.WriteString(w, `{"errcode":0,"msgid":"m-1"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "app-secret", BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return time.Unix(100, 0).UTC() }}
	value := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"}}
	if id, err := p.Deliver(context.Background(), value); err != nil || id != "m-1" {
		t.Fatalf("deliver = %q, %v", id, err)
	}
	if _, err := p.Deliver(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if tokenCalls != 1 || sendCalls != 2 {
		t.Fatalf("calls token=%d send=%d", tokenCalls, sendCalls)
	}
}
