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
}

var _ outbox.Provider = (*Provider)(nil)

// Deliver sends one durable text segment through the WeCom application API.
//
//nolint:gocyclo
func (p *Provider) Deliver(ctx context.Context, value storage.ReplyOutbox) (string, error) {
	if p == nil || strings.TrimSpace(p.CorpID) == "" || strings.TrimSpace(p.AgentID) == "" || strings.TrimSpace(p.AppSecret) == "" || ctx == nil {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	if value.ReplyTarget.ConversationKind != "direct" || value.ReplyTarget.ReceiverID == "" {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	token, err := p.accessToken(ctx)
	if err != nil {
		return "", err
	}
	agentID, parseErr := strconv.Atoi(strings.TrimSpace(p.AgentID))
	if parseErr != nil || agentID <= 0 || strconv.Itoa(agentID) != strings.TrimSpace(p.AgentID) {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	body, _ := json.Marshal(struct {
		ToUser  string `json:"touser"`
		MsgType string `json:"msgtype"`
		AgentID int    `json:"agentid"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
		Safe int `json:"safe"`
	}{value.ReplyTarget.ReceiverID, "text", agentID, struct {
		Content string `json:"content"`
	}{value.Payload}, 0})
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
			return "", &outbox.DeliveryError{Class: "canceled", Retryable: false}
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
	return result.MsgID, nil
}

// Reconcile reports unknown because WeCom does not expose a stable receipt query for app text sends.
func (p *Provider) Reconcile(context.Context, storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryUnknown, "", nil
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
