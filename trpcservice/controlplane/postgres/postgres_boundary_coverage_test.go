package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPostgreSQLRepositoriesPrioritizeCanceledContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tenants := NewTenantRepository(nil)
	agents := NewAgentRepository(nil)
	models := NewModelRepository(nil, nil)
	backends := NewBackendRepository(nil, nil)
	channelRepo := NewChannelRepository(nil)

	checks := []struct {
		name   string
		action func() error
	}{
		{name: "tenant create", action: func() error { _, err := tenants.Create(canceled, tenant.CreateInput{}); return err }},
		{name: "tenant get", action: func() error { _, err := tenants.Get(canceled, "tenant"); return err }},
		{name: "tenant update", action: func() error {
			_, err := tenants.UpdateConfiguration(canceled, tenant.UpdateConfigurationInput{})
			return err
		}},
		{name: "tenant transition", action: func() error {
			_, _, err := tenants.TransitionStatus(canceled, tenant.TransitionStatusInput{})
			return err
		}},
		{name: "agent create", action: func() error { _, err := agents.Create(canceled, agent.CreateInput{}); return err }},
		{name: "agent get", action: func() error { _, err := agents.Get(canceled, "tenant", "app"); return err }},
		{name: "agent metadata", action: func() error { _, err := agents.UpdateMetadata(canceled, agent.UpdateMetadataInput{}); return err }},
		{name: "agent create draft", action: func() error { _, err := agents.CreateDraft(canceled, agent.CreateDraftInput{}); return err }},
		{name: "agent update draft", action: func() error { _, err := agents.UpdateDraft(canceled, agent.UpdateDraftInput{}); return err }},
		{name: "agent revision", action: func() error { _, err := agents.GetRevision(canceled, "tenant", "app", 1); return err }},
		{name: "agent publish", action: func() error { _, _, _, err := agents.Publish(canceled, agent.PublishInput{}); return err }},
		{name: "agent rollback", action: func() error { _, _, err := agents.Rollback(canceled, agent.RollbackInput{}); return err }},
		{name: "agent transition", action: func() error {
			_, _, err := agents.TransitionStatus(canceled, agent.TransitionStatusInput{})
			return err
		}},
		{name: "model create", action: func() error { _, _, err := models.Create(canceled, model.CreateInput{}); return err }},
		{name: "model get", action: func() error { _, err := models.Get(canceled, "tenant", "profile"); return err }},
		{name: "model update", action: func() error {
			_, _, err := models.UpdateConfiguration(canceled, model.UpdateConfigurationInput{})
			return err
		}},
		{name: "model transition", action: func() error {
			_, _, err := models.TransitionStatus(canceled, model.TransitionStatusInput{})
			return err
		}},
		{name: "backend create", action: func() error { _, _, err := backends.Create(canceled, backend.CreateInput{}); return err }},
		{name: "backend get", action: func() error { _, err := backends.Get(canceled, "tenant", "profile"); return err }},
		{name: "backend update", action: func() error {
			_, _, err := backends.UpdateConfiguration(canceled, backend.UpdateConfigurationInput{})
			return err
		}},
		{name: "backend transition", action: func() error {
			_, _, err := backends.TransitionStatus(canceled, backend.TransitionStatusInput{})
			return err
		}},
		{name: "channel create", action: func() error { _, _, err := channelRepo.Create(canceled, channels.CreateInput{}); return err }},
		{name: "channel get", action: func() error { _, err := channelRepo.Get(canceled, "tenant", "binding"); return err }},
		{name: "channel update", action: func() error {
			_, _, err := channelRepo.UpdateConfiguration(canceled, channels.UpdateConfigurationInput{})
			return err
		}},
		{name: "channel transition", action: func() error {
			_, _, err := channelRepo.TransitionStatus(canceled, channels.TransitionStatusInput{})
			return err
		}},
		{name: "channel candidate lookup", action: func() error {
			_, err := channelRepo.LookupCandidates(canceled, channels.ChannelTelegram, "invalid")
			return err
		}},
		{name: "channel candidate consume", action: func() error {
			_, err := channelRepo.ConsumeCandidate(canceled, channels.CandidateBindingContext{})
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.action(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		})
	}
}

func TestPostgreSQLNilReceiversFailClosed(t *testing.T) {
	var tenants *TenantRepository
	var agents *AgentRepository
	var models *ModelRepository
	var backends *BackendRepository
	var channelRepo *ChannelRepository
	checks := []struct {
		name   string
		action func() error
	}{
		{name: "tenant", action: func() error { _, err := tenants.Get(context.Background(), "tenant"); return err }},
		{name: "agent app", action: func() error { _, err := agents.Get(context.Background(), "tenant", "app"); return err }},
		{name: "agent revision", action: func() error { _, err := agents.GetRevision(context.Background(), "tenant", "app", 1); return err }},
		{name: "model", action: func() error { _, err := models.Get(context.Background(), "tenant", "profile"); return err }},
		{name: "backend", action: func() error { _, err := backends.Get(context.Background(), "tenant", "profile"); return err }},
		{name: "channel", action: func() error { _, err := channelRepo.Get(context.Background(), "tenant", "binding"); return err }},
		{name: "channel candidate lookup", action: func() error {
			_, err := channelRepo.LookupCandidates(context.Background(), channels.ChannelTelegram, strings.Repeat("a", 64))
			return err
		}},
		{name: "channel candidate consume", action: func() error {
			_, err := channelRepo.ConsumeCandidate(context.Background(), channels.CandidateBindingContext{})
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.action(); !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v, want ErrStorage", err)
			}
		})
	}
}
