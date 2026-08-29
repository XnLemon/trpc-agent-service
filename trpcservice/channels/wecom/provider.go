package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// Provider delivers text replies through a WeCom self-built application.
// Access tokens are cached in memory and are never persisted or logged.
type Provider struct {
	CorpID      string
	AgentID     string
	AppSecret   string
	HTTPClient  *http.Client
	BaseURL     string
	Now         func() time.Time
	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
	receipts    map[string]string
}

var _ outbox.Provider = (*Provider)(nil)

const maximumTextBytes = 2048

// Deliver sends one durable text segment through the WeCom application API.
//
//nolint:gocyclo
func (p *Provider) Deliver(ctx context.Context, value storage.ReplyOutbox) (string, error) {
	if p == nil || strings.TrimSpace(p.CorpID) == "" || strings.TrimSpace(p.AgentID) == "" || strings.TrimSpace(p.AppSecret) == "" || ctx == nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	if (value.ReplyTarget.ConversationKind != "direct" && value.ReplyTarget.ConversationKind != "group") || value.ReplyTarget.ReceiverID == "" {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	if len([]byte(value.Payload)) == 0 || len([]byte(value.Payload)) > maximumTextBytes {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	agentID, parseErr := strconv.Atoi(strings.TrimSpace(p.AgentID))
	if parseErr != nil || agentID <= 0 || strconv.Itoa(agentID) != strings.TrimSpace(p.AgentID) {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	key := deliveryKey(value)
	p.mu.Lock()
	if value.ReplyID != "" && p.receipts != nil {
		if receipt := p.receipts[key]; receipt != "" {
			p.mu.Unlock()
			return receipt, nil
		}
	}
	p.mu.Unlock()
	token, err := p.accessToken(ctx)
	if err != nil {
		return "", err
	}
	target := struct {
		ToUser  string `json:"touser,omitempty"`
		ChatID  string `json:"chatid,omitempty"`
		MsgType string `json:"msgtype"`
		AgentID int    `json:"agentid"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
		Safe int `json:"safe"`
	}{MsgType: "text", AgentID: agentID, Text: struct {
		Content string `json:"content"`
	}{value.Payload}, Safe: 0}
	if value.ReplyTarget.ConversationKind == "group" {
		target.ChatID = value.ReplyTarget.ReceiverID
	} else {
		target.ToUser = value.ReplyTarget.ReceiverID
	}
	body, _ := json.Marshal(target)
	endpoint := p.baseURL() + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client().Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", &outbox.DeliveryError{Class: "timeout", Retryable: true}
		}
		if errors.Is(err, context.Canceled) {
			return "", &outbox.DeliveryError{Class: "canceled", Retryable: true}
		}
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
		MsgID   string `json:"msgid"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	if result.ErrCode != 0 {
		class, retryable := classifyWeCom(result.ErrCode, response.StatusCode)
		if class == "unauthenticated" {
			p.mu.Lock()
			p.token = ""
			p.tokenExpiry = time.Time{}
			p.mu.Unlock()
		}
		return "", &outbox.DeliveryError{Class: class, Retryable: retryable}
	}
	if result.MsgID == "" {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	p.mu.Lock()
	if value.ReplyID != "" {
		if p.receipts == nil {
			p.receipts = make(map[string]string)
		}
		p.receipts[key] = result.MsgID
	}
	p.mu.Unlock()
	return result.MsgID, nil
}

// Reconcile reports unknown because WeCom does not expose a stable receipt query for app text sends.
func (p *Provider) Reconcile(_ context.Context, value storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if p == nil {
		return outbox.DeliveryUnknown, "", nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if receipt := p.receipts[deliveryKey(value)]; receipt != "" {
		return outbox.DeliveryAccepted, receipt, nil
	}
	return outbox.DeliveryUnknown, "", nil
}

func deliveryKey(value storage.ReplyOutbox) string {
	return value.TenantID + "\x00" + value.ReplyID + "\x00" + strconv.Itoa(value.SegmentIndex)
}

func (p *Provider) accessToken(ctx context.Context) (string, error) {
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	p.mu.Lock()
	if p.token != "" && now.Before(p.tokenExpiry) {
		token := p.token
		p.mu.Unlock()
		return token, nil
	}
	p.mu.Unlock()
	endpoint := p.baseURL() + "/cgi-bin/gettoken?corpid=" + url.QueryEscape(p.CorpID) + "&corpsecret=" + url.QueryEscape(p.AppSecret)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	response, err := p.client().Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", &outbox.DeliveryError{Class: "timeout", Retryable: true}
		}
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	var result struct {
		ErrCode     int    `json:"errcode"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return "", &outbox.DeliveryError{Class: "provider_error", Retryable: true}
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		class, retryable := classifyWeCom(result.ErrCode, response.StatusCode)
		return "", &outbox.DeliveryError{Class: class, Retryable: retryable}
	}
	expires := now.Add(time.Duration(result.ExpiresIn) * time.Second)
	if result.ExpiresIn <= 60 {
		expires = now.Add(30 * time.Second)
	} else {
		expires = expires.Add(-30 * time.Second)
	}
	p.mu.Lock()
	p.token, p.tokenExpiry = result.AccessToken, expires
	p.mu.Unlock()
	return result.AccessToken, nil
}

func (p *Provider) baseURL() string {
	if strings.TrimSpace(p.BaseURL) != "" {
		return strings.TrimRight(p.BaseURL, "/")
	}
	return "https://qyapi.weixin.qq.com"
}
func (p *Provider) client() *http.Client {
	if p.HTTPClient != nil {
		return p.HTTPClient
	}
	return http.DefaultClient
}
func classifyWeCom(code, status int) (string, bool) {
	switch {
	case code == 40014 || code == 42001:
		return "unauthenticated", true
	case code == 45009 || status == http.StatusTooManyRequests:
		return "rate_limited", true
	case code == 0 && status >= 500:
		return "unavailable", true
	case code != 0:
		return "provider_error", false
	default:
		return "provider_error", true
	}
}
