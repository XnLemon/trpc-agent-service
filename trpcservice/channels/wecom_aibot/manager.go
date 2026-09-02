package wecom_aibot

import (
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const defaultWSURL = "wss://openws.work.weixin.qq.com"

// Conn is the narrow WebSocket surface owned by Manager. It permits protocol
// tests to inject a fake connection while keeping one writer per socket.
type Conn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPongHandler(func(string) error)
	Close() error
}

type Dialer interface {
	DialContext(context.Context, string, http.Header) (Conn, error)
}

type websocketDialer struct{ dialer websocket.Dialer }

func (d websocketDialer) DialContext(ctx context.Context, url string, header http.Header) (Conn, error) {
	c, _, err := d.dialer.DialContext(ctx, url, header)
	return c, err
}

// Config wires one tenant-scoped AI Bot connection to the protocol-neutral
// Dispatcher. Secret is resolved by the owner and is never logged.
type Config struct {
	BotID             string
	Secret            string
	WSURL             string
	Target            channels.RoutingTarget
	Dispatcher        gateway.DispatchService
	Dialer            Dialer
	QueueSize         int
	HeartbeatInterval time.Duration
	ReconnectBase     time.Duration
	ReconnectMax      time.Duration
	ExecutionTimeout  time.Duration
}

// Manager owns a single long-lived connection and all of its pumps.
type Manager struct {
	botID, secret, wsURL                                     string
	target                                                   channels.RoutingTarget
	dispatcher                                               gateway.DispatchService
	dialer                                                   Dialer
	queueSize                                                int
	heartbeat, reconnectBase, reconnectMax, executionTimeout time.Duration

	mu        sync.Mutex
	conn      Conn
	queue     chan outboundFrame
	pending   *pendingReply
	authReqID string
	closing   bool
	runCancel context.CancelFunc
	runDone   chan struct{}
	ready     bool
	drains    sync.WaitGroup
}

type outboundFrame struct {
	data       []byte
	replyReqID string
	ack        chan error
	done       chan struct{}
}

type pendingReply struct {
	reqID string
	ack   chan error
	done  chan struct{}
}

func New(config Config) (*Manager, error) {
	if strings.TrimSpace(config.BotID) == "" || strings.TrimSpace(config.Secret) == "" || config.Dispatcher == nil {
		return nil, ErrInvalid
	}
	if err := config.Target.Validate(); err != nil || config.Target.Channel != channels.ChannelWeComAIBot {
		return nil, ErrInvalid
	}
	if config.WSURL == "" {
		config.WSURL = defaultWSURL
	}
	parsed, err := url.Parse(strings.TrimSpace(config.WSURL))
	if err != nil || parsed.Scheme != "wss" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, ErrInvalid
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 128
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.ReconnectBase <= 0 {
		config.ReconnectBase = time.Second
	}
	if config.ReconnectMax <= 0 {
		config.ReconnectMax = time.Minute
	}
	if config.ExecutionTimeout <= 0 {
		config.ExecutionTimeout = 4 * time.Minute
	}
	if config.Dialer == nil {
		config.Dialer = websocketDialer{dialer: websocket.Dialer{HandshakeTimeout: 10 * time.Second}}
	}
	return &Manager{botID: strings.TrimSpace(config.BotID), secret: config.Secret, wsURL: strings.TrimRight(config.WSURL, "/"), target: config.Target, dispatcher: config.Dispatcher, dialer: config.Dialer, queueSize: config.QueueSize, heartbeat: config.HeartbeatInterval, reconnectBase: config.ReconnectBase, reconnectMax: config.ReconnectMax, executionTimeout: config.ExecutionTimeout}, nil
}

func (m *Manager) Channel() channels.Channel { return channels.ChannelWeComAIBot }
func (m *Manager) Ready() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ready && !m.closing
}

func (m *Manager) Run(ctx context.Context) error {
	if m == nil || ctx == nil {
		return ErrInvalid
	}
	m.mu.Lock()
	if m.runCancel != nil {
		m.mu.Unlock()
		return ErrClosed
	}
	if m.closing {
		m.mu.Unlock()
		return ErrClosed
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.runCancel, m.runDone = cancel, make(chan struct{})
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.runCancel = nil
		done := m.runDone
		m.runDone = nil
		m.ready = false
		m.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()
	var attempt int
	for {
		if err := runCtx.Err(); err != nil {
			return err
		}
		conn, err := m.dialer.DialContext(runCtx, m.wsURL, nil)
		if err != nil {
			if !sleepBackoff(runCtx, m.reconnectBase, m.reconnectMax, attempt) {
				return runCtx.Err()
			}
			attempt++
			continue
		}
		attempt = 0
		if err := m.serveConnection(runCtx, conn); err != nil && runCtx.Err() == nil {
			_ = conn.Close()
			if !sleepBackoff(runCtx, m.reconnectBase, m.reconnectMax, attempt) {
				return runCtx.Err()
			}
			attempt++
		}
		if runCtx.Err() != nil {
			return runCtx.Err()
		}
	}
}

func sleepBackoff(ctx context.Context, base, max time.Duration, attempt int) bool {
	d := base * time.Duration(1<<min(attempt, 6))
	if d > max {
		d = max
	}
	jitter := time.Duration(rand.Int63n(int64(d/4 + 1)))
	timer := time.NewTimer(d - d/8 + jitter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (m *Manager) serveConnection(ctx context.Context, conn Conn) error {
	if conn == nil {
		return ErrClosed
	}
	m.mu.Lock()
	m.conn = conn
	m.ready = false
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if m.conn == conn {
			m.conn = nil
			m.ready = false
		}
		m.mu.Unlock()
	}()
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan outboundFrame, m.queueSize)
	m.mu.Lock()
	m.queue = queue
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if m.queue == queue {
			m.queue = nil
		}
		m.mu.Unlock()
	}()
	writeErr := make(chan error, 1)
	go m.writePump(connCtx, conn, queue, writeErr)
	if err := m.enqueueAuth(connCtx, queue); err != nil {
		return err
	}
	if err := m.readPump(connCtx, conn, queue); err != nil {
		return err
	}
	select {
	case err := <-writeErr:
		return err
	case <-connCtx.Done():
		return connCtx.Err()
	}
}

func (m *Manager) sendReply(ctx context.Context, reqID string, body StreamReply) error {
	if ctx == nil || strings.TrimSpace(reqID) == "" {
		return ErrInvalid
	}
	data, err := encodeFrame(Frame{Cmd: cmdRespond, Headers: Headers{ReqID: reqID}, Body: mustJSON(body)})
	if err != nil {
		return err
	}
	m.mu.Lock()
	queue, closing := m.queue, m.closing
	if closing || queue == nil {
		m.mu.Unlock()
		return ErrNotReady
	}
	ack := make(chan error, 1)
	done := make(chan struct{})
	select {
	case queue <- outboundFrame{data: data, replyReqID: reqID, ack: ack, done: done}:
		m.mu.Unlock()
		select {
		case err := <-ack:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		m.mu.Unlock()
		return ctx.Err()
	default:
		m.mu.Unlock()
		return ErrQueueFull
	}
}

func (m *Manager) writePump(ctx context.Context, conn Conn, queue <-chan outboundFrame, errs chan<- error) {
	defer func() {
		m.clearPendingReply(ErrClosed)
		drainOutboundReplies(queue, ErrClosed)
	}()
	for {
		select {
		case <-ctx.Done():
			errs <- ErrClosed
			return
		case outbound := <-queue:
			if outbound.data == nil {
				errs <- nil
				return
			}
			if outbound.replyReqID != "" {
				if !m.setPendingReply(outbound.replyReqID, outbound.ack, outbound.done) {
					outbound.ack <- ErrClosed
					continue
				}
			}
			if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				m.clearPendingReply(ErrClosed)
				errs <- ErrClosed
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, outbound.data); err != nil {
				_ = conn.Close()
				m.clearPendingReply(ErrClosed)
				errs <- ErrClosed
				return
			}
			if outbound.replyReqID != "" {
				select {
				case <-ctx.Done():
					m.clearPendingReply(ErrClosed)
					errs <- ErrClosed
					return
				case <-outbound.done:
				}
			}
		}
	}
}

func drainOutboundReplies(queue <-chan outboundFrame, err error) {
	for {
		select {
		case outbound := <-queue:
			if outbound.ack != nil {
				outbound.ack <- err
			}
		default:
			return
		}
	}
}

func (m *Manager) enqueueAuth(ctx context.Context, queue chan<- outboundFrame) error {
	reqID := cmdSubscribe + "-" + uuid.NewString()
	data, err := encodeFrame(Frame{Cmd: cmdSubscribe, Headers: Headers{ReqID: reqID}, Body: mustJSON(struct {
		BotID  string `json:"bot_id"`
		Secret string `json:"secret"`
	}{m.botID, m.secret})})
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.authReqID = reqID
	m.mu.Unlock()
	select {
	case queue <- outboundFrame{data: data}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) readPump(ctx context.Context, conn Conn, queue chan<- outboundFrame) error {
	_ = conn.SetReadDeadline(time.Now().Add(2 * m.heartbeat))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(2 * m.heartbeat)) })
	missedHeartbeats := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(m.heartbeat))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				missedHeartbeats++
				if missedHeartbeats >= 3 {
					return ErrClosed
				}
				ping, pingErr := encodeFrame(Frame{Cmd: cmdPing, Headers: Headers{ReqID: cmdPing + "-" + uuid.NewString()}})
				if pingErr == nil {
					select {
					case queue <- outboundFrame{data: ping}:
					default:
						return ErrQueueFull
					}
				}
				continue
			}
			return ErrClosed
		}
		frame, err := decodeFrame(data)
		if err != nil {
			continue
		}
		missedHeartbeats = 0
		if frame.Cmd == "" && m.isAuthResponse(frame.Headers.ReqID) {
			if frame.ErrCode == nil || *frame.ErrCode != 0 {
				return ErrAuthentication
			}
			m.mu.Lock()
			m.ready = true
			m.mu.Unlock()
			continue
		}
		if frame.Cmd == cmdCallback {
			if !m.Ready() || !m.beginDrain() {
				continue
			}
			go func() {
				defer m.drains.Done()
				m.handleCallback(ctx, frame)
			}()
			continue
		}
		if frame.Cmd == cmdEventCallback {
			continue
		}
		if frame.Cmd == "" {
			if frame.ErrCode != nil && *frame.ErrCode != 0 {
				m.completeReplyAck(frame.Headers.ReqID, ErrClosed)
			} else {
				m.completeReplyAck(frame.Headers.ReqID, nil)
			}
		}
	}
}

func (m *Manager) isAuthResponse(reqID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return reqID != "" && reqID == m.authReqID
}

func (m *Manager) handleCallback(parent context.Context, frame Frame) {
	var message Message
	if jsonErr := unmarshalBody(frame.Body, &message); jsonErr != nil || message.MsgID == "" || message.AIBotID != m.botID || message.MsgType != "text" || strings.TrimSpace(message.From.UserID) == "" || strings.TrimSpace(message.Text.Content) == "" {
		return
	}
	inbound := gateway.InboundMessage{Content: message.Text.Content, ContentType: gateway.ContentTypeText, ExternalMessageID: message.MsgID, ExternalUserID: message.From.UserID, ConversationKind: channels.ConversationDirect, ExternalPeerID: message.From.UserID}
	if message.ChatID != "" {
		inbound.ConversationKind = channels.ConversationGroup
		inbound.ExternalChatID = message.ChatID
		inbound.ExternalPeerID = ""
	}
	ctx, cancel := context.WithTimeout(parent, m.executionTimeout)
	defer cancel()
	stream, err := m.dispatcher.Dispatch(ctx, gateway.DispatchRequest{Principal: mustPrincipal(m.target), RequestID: frame.Headers.ReqID, Message: inbound})
	if err != nil || stream == nil {
		return
	}
	for event := range stream {
		if event.Type != gateway.DispatchEventMessage || strings.TrimSpace(event.Text) == "" {
			continue
		}
		body := StreamReply{MsgType: "stream", Stream: struct {
			ID      string `json:"id"`
			Finish  bool   `json:"finish"`
			Content string `json:"content,omitempty"`
		}{ID: frame.Headers.ReqID, Content: event.Text, Finish: false}}
		if err := m.sendReply(ctx, frame.Headers.ReqID, body); err != nil {
			return
		}
	}
}

func (m *Manager) beginDrain() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return false
	}
	m.drains.Add(1)
	return true
}

func (m *Manager) setPendingReply(reqID string, ack chan error, done chan struct{}) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.pending != nil {
		return false
	}
	m.pending = &pendingReply{reqID: reqID, ack: ack, done: done}
	return true
}

func (m *Manager) completeReplyAck(reqID string, err error) {
	m.mu.Lock()
	pending := m.pending
	if pending == nil || pending.reqID != reqID {
		m.mu.Unlock()
		return
	}
	m.pending = nil
	m.mu.Unlock()
	if pending != nil {
		pending.ack <- err
		close(pending.done)
	}
}

func (m *Manager) clearPendingReply(err error) {
	m.mu.Lock()
	pending := m.pending
	m.pending = nil
	m.mu.Unlock()
	if pending != nil {
		pending.ack <- err
		close(pending.done)
	}
}

func mustJSON(v any) []byte { data, _ := json.Marshal(v); return data }
func unmarshalBody(data []byte, out any) error {
	if len(data) == 0 {
		return ErrMalformedFrame
	}
	return json.Unmarshal(data, out)
}
func mustPrincipal(target channels.RoutingTarget) gateway.Principal {
	p, _ := gateway.NewChannelPrincipal(target)
	return p
}

func (m *Manager) BeginShutdown() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closing = true
	cancel := m.runCancel
	conn := m.conn
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	m.clearPendingReply(ErrClosed)
}
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.BeginShutdown()
	m.mu.Lock()
	done := m.runDone
	m.mu.Unlock()
	if done != nil {
		<-done
	}
	m.drains.Wait()
	return nil
}

var _ channels.PollingAdapter = (*Manager)(nil)
