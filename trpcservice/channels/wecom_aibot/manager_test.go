package wecom_aibot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestManagerAuthenticatesAndMapsDirectAndGroupCallbacks(t *testing.T) {
	connection := newTestConn()
	dispatcher := &testDispatcher{requests: make(chan gateway.DispatchRequest, 2)}
	dispatcher.events = func() <-chan gateway.DispatchEvent {
		result := make(chan gateway.DispatchEvent, 2)
		result <- gateway.DispatchEvent{Type: gateway.DispatchEventMessage, Text: "partial"}
		result <- gateway.DispatchEvent{Type: gateway.DispatchEventDone, Done: true}
		close(result)
		return result
	}
	manager, stop := startTestManager(t, connection, dispatcher, 4)
	defer stop()

	auth := readFrame(t, connection.writes)
	if auth.Cmd != cmdSubscribe {
		t.Fatalf("auth command = %q", auth.Cmd)
	}
	var credentials struct {
		BotID  string `json:"bot_id"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(auth.Body, &credentials); err != nil || credentials.BotID != "bot-1" || credentials.Secret != "secret-1" {
		t.Fatalf("auth payload is invalid")
	}
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	connection.reads <- callbackFrame(t, "req-direct", "msg-direct", "", "user-1")
	direct := <-dispatcher.requests
	if direct.RequestID != "req-direct" || direct.Message.ConversationKind != channels.ConversationDirect || direct.Message.ExternalPeerID != "user-1" || direct.Message.ExternalMessageID != "msg-direct" {
		t.Fatalf("direct request = %+v", direct)
	}
	partial := readFrame(t, connection.writes)
	if partial.Cmd != cmdRespond || partial.Headers.ReqID != "req-direct" {
		t.Fatalf("direct reply = %+v", partial)
	}
	connection.reads <- ackFrame(t, "req-direct", 0)

	connection.reads <- callbackFrame(t, "req-group", "msg-group", "chat-1", "user-2")
	group := <-dispatcher.requests
	if group.RequestID != "req-group" || group.Message.ConversationKind != channels.ConversationGroup || group.Message.ExternalChatID != "chat-1" || group.Message.ExternalPeerID != "" {
		t.Fatalf("group request = %+v", group)
	}
	connection.reads <- ackFrame(t, readFrame(t, connection.writes).Headers.ReqID, 0)
}

func TestProviderWaitsForCorrelatedFinalReplyAcknowledgement(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	provider, err := NewProvider(manager, testCorrelations{requestID: "req-final"})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, deliverErr := provider.Deliver(context.Background(), storage.ReplyOutbox{TenantID: "tenant", EventID: "event", ReplyID: "reply", SegmentIndex: 0, Payload: "final"})
		result <- deliverErr
	}()
	final := readFrame(t, connection.writes)
	if final.Cmd != cmdRespond || final.Headers.ReqID != "req-final" {
		t.Fatalf("final frame = %+v", final)
	}
	var body StreamReply
	if err := json.Unmarshal(final.Body, &body); err != nil || !body.Stream.Finish || body.Stream.ID != "req-final" {
		t.Fatalf("final body = %+v", body)
	}
	select {
	case err := <-result:
		t.Fatalf("delivery completed before ack: %v", err)
	default:
	}
	connection.reads <- ackFrame(t, "req-final", 0)
	if err := <-result; err != nil {
		t.Fatalf("delivery error = %v", err)
	}
}

func TestManagerBoundsReplyQueueAndReconnects(t *testing.T) {
	first, second := newTestConn(), newTestConn()
	dialer := &testDialer{connections: make(chan Conn, 2)}
	dialer.connections <- first
	dialer.connections <- second
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 1, heartbeat: time.Hour, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	auth := readFrame(t, first.writes)
	first.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)

	firstReply := make(chan error, 1)
	go func() { firstReply <- manager.sendReply(context.Background(), "req-1", StreamReply{}) }()
	_ = readFrame(t, first.writes)
	secondReply := make(chan error, 1)
	go func() { secondReply <- manager.sendReply(context.Background(), "req-2", StreamReply{}) }()
	waitQueueFull(t, manager)
	if err := manager.sendReply(context.Background(), "req-3", StreamReply{}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue error = %v", err)
	}
	first.reads <- ackFrame(t, "req-1", 0)
	if err := <-firstReply; err != nil {
		t.Fatalf("first reply error = %v", err)
	}
	_ = readFrame(t, first.writes)
	first.reads <- ackFrame(t, "req-2", 0)
	if err := <-secondReply; err != nil {
		t.Fatalf("second reply error = %v", err)
	}
	_ = first.Close()
	auth = readFrame(t, second.writes)
	second.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	manager.BeginShutdown()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop")
	}
	if writes := first.maxWrites.Load(); writes > 1 {
		t.Fatalf("concurrent writes = %d", writes)
	}
}

func TestManagerSendsHeartbeatAndAcceptsAcknowledgement(t *testing.T) {
	connection := newHeartbeatConn()
	dialer := &testDialer{connections: make(chan Conn, 1)}
	dialer.connections <- connection
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dialer: dialer, queueSize: 2, heartbeat: 10 * time.Millisecond, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	ping := readFrame(t, connection.writes)
	if ping.Cmd != cmdPing {
		t.Fatalf("heartbeat command = %q", ping.Cmd)
	}
	connection.reads <- ackFrame(t, ping.Headers.ReqID, 0)
	if !manager.Ready() {
		t.Fatal("heartbeat acknowledgement made manager unavailable")
	}
	manager.BeginShutdown()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop")
	}
}

func TestPendingReplyFailsWhenConnectionCloses(t *testing.T) {
	connection := newTestConn()
	manager, stop := startTestManager(t, connection, &testDispatcher{}, 2)
	defer stop()
	auth := readFrame(t, connection.writes)
	connection.reads <- ackFrame(t, auth.Headers.ReqID, 0)
	waitReady(t, manager)
	result := make(chan error, 1)
	go func() { result <- manager.sendReply(context.Background(), "req-close", StreamReply{}) }()
	_ = readFrame(t, connection.writes)
	_ = connection.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("pending reply error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending reply was not failed after close")
	}
}

type testDispatcher struct {
	requests chan gateway.DispatchRequest
	events   func() <-chan gateway.DispatchEvent
}

func (d *testDispatcher) Dispatch(_ context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	if d.requests != nil {
		d.requests <- request
	}
	if d.events == nil {
		result := make(chan gateway.DispatchEvent)
		close(result)
		return result, nil
	}
	return d.events(), nil
}

type testCorrelations struct{ requestID string }

func (c testCorrelations) GetReplyCorrelation(context.Context, string, string) (storage.ReplyCorrelation, error) {
	return storage.ReplyCorrelation{RequestID: c.requestID}, nil
}

type testDialer struct{ connections chan Conn }

func (d *testDialer) DialContext(ctx context.Context, _ string, _ http.Header) (Conn, error) {
	select {
	case connection := <-d.connections:
		return connection, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type testConn struct {
	reads     chan []byte
	writes    chan []byte
	closed    chan struct{}
	closeOnce sync.Once
	writesNow atomic.Int32
	maxWrites atomic.Int32
}

func newTestConn() *testConn {
	return &testConn{reads: make(chan []byte, 16), writes: make(chan []byte, 16), closed: make(chan struct{})}
}
func (c *testConn) ReadMessage() (int, []byte, error) {
	select {
	case data := <-c.reads:
		return 1, data, nil
	case <-c.closed:
		return 0, nil, errors.New("closed")
	}
}
func (c *testConn) WriteMessage(_ int, data []byte) error {
	active := c.writesNow.Add(1)
	for {
		maximum := c.maxWrites.Load()
		if active <= maximum || c.maxWrites.CompareAndSwap(maximum, active) {
			break
		}
	}
	defer c.writesNow.Add(-1)
	select {
	case c.writes <- data:
		return nil
	case <-c.closed:
		return errors.New("closed")
	}
}
func (*testConn) SetReadDeadline(time.Time) error   { return nil }
func (*testConn) SetWriteDeadline(time.Time) error  { return nil }
func (*testConn) SetPongHandler(func(string) error) {}
func (c *testConn) Close() error                    { c.closeOnce.Do(func() { close(c.closed) }); return nil }

type heartbeatConn struct {
	*testConn
	mu       sync.Mutex
	deadline time.Time
}

func newHeartbeatConn() *heartbeatConn { return &heartbeatConn{testConn: newTestConn()} }
func (c *heartbeatConn) SetReadDeadline(value time.Time) error {
	c.mu.Lock()
	c.deadline = value
	c.mu.Unlock()
	return nil
}
func (c *heartbeatConn) ReadMessage() (int, []byte, error) {
	c.mu.Lock()
	deadline := c.deadline
	c.mu.Unlock()
	if deadline.IsZero() {
		return c.testConn.ReadMessage()
	}
	delay := time.Until(deadline)
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case data := <-c.reads:
		return 1, data, nil
	case <-c.closed:
		return 0, nil, errors.New("closed")
	case <-timer.C:
		return 0, nil, timeoutError{}
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func startTestManager(t *testing.T, connection *testConn, dispatcher gateway.DispatchService, queueSize int) (*Manager, func()) {
	t.Helper()
	dialer := &testDialer{connections: make(chan Conn, 1)}
	dialer.connections <- connection
	manager := &Manager{botID: "bot-1", secret: "secret-1", wsURL: defaultWSURL, dispatcher: dispatcher, dialer: dialer, queueSize: queueSize, heartbeat: time.Hour, reconnectBase: time.Millisecond, reconnectMax: time.Millisecond, executionTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	return manager, func() {
		manager.BeginShutdown()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("manager did not stop")
		}
	}
}

func readFrame(t *testing.T, input <-chan []byte) Frame {
	t.Helper()
	select {
	case data := <-input:
		frame, err := decodeFrame(data)
		if err != nil {
			t.Fatal(err)
		}
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for frame")
		return Frame{}
	}
}
func ackFrame(t *testing.T, reqID string, code int) []byte {
	t.Helper()
	frame := Frame{Headers: Headers{ReqID: reqID}, ErrCode: &code}
	data, err := encodeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func callbackFrame(t *testing.T, reqID, msgID, chatID, userID string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"msgid": msgID, "aibotid": "bot-1", "chatid": chatID, "msgtype": "text", "from": map[string]string{"userid": userID}, "text": map[string]string{"content": "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeFrame(Frame{Cmd: cmdCallback, Headers: Headers{ReqID: reqID}, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func waitReady(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if manager.Ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("manager never became ready")
}
func waitQueueFull(t *testing.T, manager *Manager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		queued := manager.queue != nil && len(manager.queue) == cap(manager.queue)
		manager.mu.Unlock()
		if queued {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("reply queue never filled")
}

var _ outbox.Provider = (*Provider)(nil)
