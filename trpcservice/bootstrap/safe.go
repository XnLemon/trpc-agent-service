package bootstrap

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/XnLemon/trpc-agent-service/internal/nilvalue"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// errBootstrapCallback is deliberately stable: callback panic values can be
// arbitrary objects (including credentials) and must not escape construction
// or lifecycle boundaries.
var errBootstrapCallback = errors.New("bootstrap callback failed")

func callBootstrapError(callback func() error) (err error) {
	if callback == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return callback()
}

func callBootstrapBool(callback func() bool) (value bool) {
	if callback == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			value = false
		}
	}()
	return callback()
}

func callBootstrapContextError(callback func(context.Context) error, ctx context.Context) (err error) {
	if callback == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return callback(ctx)
}

func callBootstrapMigration(callback func(context.Context, any) error, ctx context.Context, database any) (err error) {
	if callback == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return callback(ctx, database)
}

func callBootstrapHandlerFactory(callback func(gateway.DispatchService) (http.Handler, error), dispatcher gateway.DispatchService) (handler http.Handler, err error) {
	if callback == nil {
		return nil, errBootstrapCallback
	}
	defer func() {
		if recover() != nil {
			handler = nil
			err = errBootstrapCallback
		}
	}()
	return callback(dispatcher)
}

func callBootstrapPollingFactory(callback func(gateway.DispatchService) (channels.PollingAdapter, error), dispatcher gateway.DispatchService) (adapter channels.PollingAdapter, err error) {
	if callback == nil {
		return nil, errBootstrapCallback
	}
	defer func() {
		if recover() != nil {
			adapter = nil
			err = errBootstrapCallback
		}
	}()
	return callback(dispatcher)
}

func callBootstrapWorkerFactory(callback func([]channels.PollingAdapter) (*outbox.Worker, error), adapters []channels.PollingAdapter) (worker *outbox.Worker, err error) {
	if callback == nil {
		return nil, errBootstrapCallback
	}
	defer func() {
		if recover() != nil {
			worker = nil
			err = errBootstrapCallback
		}
	}()
	return callback(adapters)
}

func callPollingChannel(adapter channels.PollingAdapter) (channel channels.Channel) {
	if nilvalue.Is(adapter) {
		return ""
	}
	defer func() {
		if recover() != nil {
			channel = ""
		}
	}()
	return adapter.Channel()
}

func callPollingReady(value pollingHealth) (ready bool) {
	if nilvalue.Is(value) {
		return false
	}
	defer func() {
		if recover() != nil {
			ready = false
		}
	}()
	return value.Ready()
}

func callPollingRun(adapter channels.PollingAdapter, ctx context.Context) (err error) {
	if nilvalue.Is(adapter) {
		return errBootstrapCallback
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return adapter.Run(ctx)
}

func closePollingAdapter(adapter channels.PollingAdapter) (err error) {
	if nilvalue.Is(adapter) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return adapter.Close()
}

func beginShutdownSafely(lifecycle callbackLifecycle) {
	if nilvalue.Is(lifecycle) {
		return
	}
	defer func() { _ = recover() }()
	lifecycle.BeginShutdown()
}

func callBootstrapBeginShutdown(lifecycle interface{ BeginShutdown() }) {
	if nilvalue.Is(lifecycle) {
		return
	}
	defer func() { _ = recover() }()
	lifecycle.BeginShutdown()
}

func closeLifecycleSafely(lifecycle callbackLifecycle) (err error) {
	if nilvalue.Is(lifecycle) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return lifecycle.Close()
}

func closeHandlerSafely(handler http.Handler) (err error) {
	if nilvalue.Is(handler) {
		return nil
	}
	lifecycle, ok := handler.(callbackLifecycle)
	if !ok || nilvalue.Is(lifecycle) {
		return nil
	}
	return closeLifecycleSafely(lifecycle)
}

func closeWorkerSafely(worker interface{ Close() error }) (err error) {
	if nilvalue.Is(worker) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return worker.Close()
}

func safeTelemetryShutdown(provider observability.Provider, ctx context.Context) (err error) {
	if nilvalue.Is(provider) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errBootstrapCallback
		}
	}()
	return provider.Shutdown(ctx)
}

func recoverStaleSafely(store storage.ToolInvocationRecoveryStore, ctx context.Context, before time.Time) (values []storage.ToolInvocation, err error) {
	if nilvalue.Is(store) || nilvalue.Is(ctx) {
		return nil, errBootstrapCallback
	}
	defer func() {
		if recover() != nil {
			values = nil
			err = errBootstrapCallback
		}
	}()
	return store.RecoverStaleToolInvocations(ctx, before)
}

func listReconciliationSafely(store storage.ToolInvocationReconciliationStore, ctx context.Context) (values []storage.ToolInvocation, err error) {
	if nilvalue.Is(store) || nilvalue.Is(ctx) {
		return nil, errBootstrapCallback
	}
	defer func() {
		if recover() != nil {
			values = nil
			err = errBootstrapCallback
		}
	}()
	return store.ListToolInvocationReconciliation(ctx)
}

func listAuditCandidatesSafely(store storage.ToolInvocationAuditStore, ctx context.Context) (values []storage.ToolInvocation, err error) {
	if nilvalue.Is(store) || nilvalue.Is(ctx) {
		return nil, errBootstrapCallback
	}
	defer func() {
		if recover() != nil {
			values = nil
			err = errBootstrapCallback
		}
	}()
	return store.ListToolInvocationAuditCandidates(ctx)
}
