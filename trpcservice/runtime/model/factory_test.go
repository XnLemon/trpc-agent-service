package modelruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestResolveAndBuildUsesExplicitConditionalTenantSecretScope(t *testing.T) {
	input := testModelFactoryInput("")
	resolver := &recordingResolver{}
	factory := &recordingFactory{}
	if _, err := ResolveAndBuild(context.Background(), input, nil, factory); err != nil {
		t.Fatalf("optional no-secret build error = %v", err)
	}
	if factory.calls != 1 || factory.secret.Value() != "" {
		t.Fatalf("optional no-secret factory calls=%d secret=%q", factory.calls, factory.secret.Value())
	}
	if resolver.calls != 0 {
		t.Fatalf("optional no-secret resolver calls = %d, want zero", resolver.calls)
	}

	input.SecretRef = "secret://tenant/model"
	secret, err := NewSecretValue("super-secret")
	if err != nil {
		t.Fatal(err)
	}
	resolver.value = secret
	factory = &recordingFactory{}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, factory); err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 || resolver.scope != (SecretScope{TenantID: input.TenantID, SecretRef: input.SecretRef}) {
		t.Fatalf("unexpected resolver scope/calls: %+v calls=%d", resolver.scope, resolver.calls)
	}
	if factory.secret.Value() != "super-secret" || factory.input.SecretRef != input.SecretRef {
		t.Fatalf("secret was not passed only to factory: value=%q input=%+v", factory.secret.Value(), factory.input)
	}
	if fmt.Sprint(factory.secret) != "<redacted-secret>" {
		t.Fatalf("secret String() leaked value: %q", fmt.Sprint(factory.secret))
	}
}

func TestResolveAndBuildCleansPartiallyBuiltModelsAndFailsClosed(t *testing.T) {
	input := testModelFactoryInput("secret://tenant/model")
	secret, err := NewSecretValue("secret")
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingResolver{value: secret}
	var closed atomic.Int32
	factory := &recordingFactory{model: &closableFakeModel{closed: &closed}, err: errors.New("provider detail")}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, factory); !errors.Is(err, ErrModelFactory) {
		t.Fatalf("factory partial result error = %v", err)
	}
	if closed.Load() != 1 {
		t.Fatalf("partial model close count = %d, want 1", closed.Load())
	}

	factory = &recordingFactory{panicCall: true}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, factory); !errors.Is(err, ErrModelFactory) {
		t.Fatalf("factory panic error = %v", err)
	}
	resolver.panicCall = true
	if _, err := ResolveAndBuild(context.Background(), input, resolver, &recordingFactory{}); !errors.Is(err, ErrSecretResolution) {
		t.Fatalf("resolver panic error = %v", err)
	}
	resolver.panicCall = false
	resolver.value = SecretValue{}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, &recordingFactory{}); !errors.Is(err, ErrSecretResolution) {
		t.Fatalf("empty resolved secret error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	factory = &recordingFactory{model: &closableFakeModel{closed: &closed}, cancel: cancel}
	if _, err := ResolveAndBuild(ctx, testModelFactoryInput(""), nil, factory); !errors.Is(err, context.Canceled) {
		t.Fatalf("late cancellation error = %v", err)
	}
	if closed.Load() != 2 {
		t.Fatalf("canceled model close count = %d, want 2", closed.Load())
	}
}

func TestResolveAndBuildRedactsErrorsAndHonorsBoundaries(t *testing.T) {
	input := testModelFactoryInput("secret://tenant/model")
	resolver := &recordingResolver{err: errors.New("KMS returned super-secret")}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, &recordingFactory{}); !errors.Is(err, ErrSecretResolution) || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("resolver error was not redacted/classified: %v", err)
	}
	resolver.err = nil
	resolver.value, _ = NewSecretValue("super-secret")
	factory := &recordingFactory{err: errors.New("provider rejected super-secret")}
	if _, err := ResolveAndBuild(context.Background(), input, resolver, factory); !errors.Is(err, ErrModelFactory) || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("factory error was not redacted/classified: %v", err)
	}
	if _, err := ResolveAndBuild(nil, input, nil, &recordingFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := ResolveAndBuild(context.Background(), input, nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil factory error = %v", err)
	}
	if _, err := ResolveAndBuild(context.Background(), ModelFactoryInput{}, nil, &recordingFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("incomplete factory input error = %v", err)
	}
	if _, err := ResolveAndBuild(context.Background(), testModelFactoryInput(""), nil, &recordingFactory{returnNil: true}); !errors.Is(err, ErrModelFactory) {
		t.Fatalf("nil model error = %v", err)
	}
	if _, err := ResolveAndBuild(context.Background(), input, nil, &recordingFactory{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing resolver error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ResolveAndBuild(canceled, input, resolver, &recordingFactory{err: errors.New("factory failure")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build error = %v", err)
	}
}

func testModelFactoryInput(secretRef string) ModelFactoryInput {
	return ModelFactoryInput{
		TenantID: "t_00000000000000000000000000", TenantVersion: 1,
		ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileVersion: 1,
		ContentDigest: "model-digest", SchemaVersion: SchemaVersionV1,
		Provider: "fake", Model: "deterministic", SecretRef: secretRef,
	}
}

type recordingResolver struct {
	calls     int
	scope     SecretScope
	value     SecretValue
	err       error
	panicCall bool
}

func (resolver *recordingResolver) Resolve(_ context.Context, scope SecretScope) (SecretValue, error) {
	resolver.calls++
	if resolver.panicCall {
		panic("resolver panic")
	}
	resolver.scope = scope
	if resolver.err != nil {
		return SecretValue{}, resolver.err
	}
	return resolver.value, nil
}

type recordingFactory struct {
	calls     int
	input     ModelFactoryInput
	secret    SecretValue
	err       error
	returnNil bool
	model     trpcmodel.Model
	panicCall bool
	cancel    context.CancelFunc
}

func (factory *recordingFactory) New(_ context.Context, input ModelFactoryInput, secret SecretValue) (trpcmodel.Model, error) {
	factory.calls++
	if factory.panicCall {
		panic("factory panic")
	}
	if factory.cancel != nil {
		factory.cancel()
	}
	factory.input = input
	factory.secret = secret
	if factory.err != nil {
		return factory.model, factory.err
	}
	if factory.returnNil {
		return nil, nil
	}
	if factory.model != nil {
		return factory.model, nil
	}
	return fakeModel{}, nil
}

type closableFakeModel struct {
	closed     *atomic.Int32
	panicClose bool
}

func (model *closableFakeModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "closable"} }

func (model *closableFakeModel) GenerateContent(ctx context.Context, _ *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	responses := make(chan *trpcmodel.Response)
	close(responses)
	return responses, nil
}

func (model *closableFakeModel) Close() error {
	if model.closed != nil {
		model.closed.Add(1)
	}
	if model.panicClose {
		panic("closer panic")
	}
	return nil
}

type fakeModel struct{}

func (fakeModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "chat"} }

func (fakeModel) GenerateContent(ctx context.Context, _ *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	responses := make(chan *trpcmodel.Response, 1)
	select {
	case responses <- &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.NewAssistantMessage("ok")}}, Done: true}:
	case <-ctx.Done():
	}
	close(responses)
	return responses, nil
}
