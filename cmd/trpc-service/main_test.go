package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

func TestServiceOptionsAndServerDefaults(t *testing.T) {
	options, help, err := parseServiceOptions(nil, io.Discard)
	if err != nil || help || options.address != defaultListenAddress || options.shutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("default options = %+v help=%v err=%v", options, help, err)
	}
	options, help, err = parseServiceOptions([]string{"-h"}, io.Discard)
	if err != nil || !help {
		t.Fatalf("help options = %+v help=%v err=%v", options, help, err)
	}
	options, help, err = parseServiceOptions([]string{"-addr", "127.0.0.1:0", "-shutdown-timeout", "2s"}, io.Discard)
	if err != nil || help || options.address != "127.0.0.1:0" || options.shutdownTimeout != 2*time.Second {
		t.Fatalf("custom options = %+v help=%v err=%v", options, help, err)
	}
	for _, args := range [][]string{{"-addr", ""}, {"-shutdown-timeout", "0"}, {"unexpected"}} {
		if _, _, err := parseServiceOptions(args, io.Discard); err == nil {
			t.Fatalf("invalid options were accepted: %v", args)
		}
	}
	server := newServiceHTTPServer(http.NotFoundHandler(), options)
	if server.Addr != options.address || server.Handler == nil || server.ReadHeaderTimeout <= 0 || server.ReadTimeout <= 0 || server.WriteTimeout <= 0 || server.IdleTimeout <= 0 {
		t.Fatalf("server defaults = %+v", server)
	}
}

func TestRunMainHelpAndSupervisorShutdown(t *testing.T) {
	var output strings.Builder
	if err := runMain(context.Background(), []string{"--help"}, &output, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "usage:") {
		t.Fatalf("help output = %q", output.String())
	}

	handler, err := gateway.NewHTTPHandler(gateway.HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	serveStarted := make(chan struct{})
	serveRelease := make(chan struct{})
	serve := func() error {
		close(serveStarted)
		<-serveRelease
		return http.ErrServerClosed
	}
	shutdownCalled := make(chan context.Context, 1)
	shutdown := func(ctx context.Context) error {
		shutdownCalled <- ctx
		return nil
	}
	signals := make(chan os.Signal, 1)
	result := make(chan error, 1)
	go func() { result <- runService(context.Background(), signals, handler, time.Second, serve, shutdown) }()
	<-serveStarted
	signals <- syscall.SIGTERM
	select {
	case shutdownContext := <-shutdownCalled:
		if _, ok := shutdownContext.Deadline(); !ok {
			t.Fatal("shutdown context has no deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown was not called")
	}
	if handler.Ready() {
		t.Fatal("handler remained ready after signal shutdown")
	}
	close(serveRelease)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestRunMainStartsAndStopsConfiguredHTTPServer(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"trpc-service", "--help"}
	main()
	os.Args = oldArgs

	signals := make(chan os.Signal, 1)
	var output strings.Builder
	result := make(chan error, 1)
	go func() {
		result <- runMain(context.Background(), []string{"-addr", "127.0.0.1:0", "-shutdown-timeout", "500ms"}, &output, io.Discard, signals)
	}()
	time.Sleep(100 * time.Millisecond)
	signals <- syscall.SIGTERM
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("configured HTTP server did not stop")
	}
	if !strings.Contains(output.String(), "listening on 127.0.0.1:0") {
		t.Fatalf("server output = %q", output.String())
	}
}

func TestRunServiceContextAndServeErrors(t *testing.T) {
	handler, err := gateway.NewHTTPHandler(gateway.HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	shutdownCalled := make(chan struct{})
	serve := func() error {
		<-ctx.Done()
		return nil
	}
	shutdown := func(context.Context) error {
		close(shutdownCalled)
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- runService(ctx, nil, handler, time.Second, serve, shutdown) }()
	cancel()
	select {
	case <-shutdownCalled:
	case <-time.After(time.Second):
		t.Fatal("context shutdown was not called")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	serveError := errors.New("bind failed")
	err = runService(context.Background(), nil, handler, time.Second, func() error { return serveError }, func(context.Context) error { return nil })
	if !errors.Is(err, serveError) || !strings.Contains(err.Error(), "bind failed") {
		t.Fatalf("serve error = %v", err)
	}
	if handler.Ready() {
		t.Fatal("handler remained ready after serve failure")
	}
	if err := runService(context.Background(), nil, nil, time.Second, nil, nil); err == nil {
		t.Fatal("invalid supervisor configuration was accepted")
	}
}
