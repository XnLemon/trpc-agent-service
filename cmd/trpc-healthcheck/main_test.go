package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCheckAcceptsSuccessfulEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Fatalf("method = %s", request.Method)
		}
		_, _ = io.WriteString(writer, "ok\n")
	}))
	defer server.Close()

	if err := check(context.Background(), server.Client(), server.URL); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRejectsInvalidInputsAndUnhealthyResponses(t *testing.T) {
	tests := []struct {
		name    string
		handler http.Handler
		parent  context.Context
		client  *http.Client
		url     string
	}{
		{name: "nil context", parent: nil, client: http.DefaultClient, url: "http://127.0.0.1"},
		{name: "nil client", parent: context.Background(), url: "http://127.0.0.1"},
		{name: "empty URL", parent: context.Background(), client: http.DefaultClient},
		{name: "server error", parent: context.Background(), client: http.DefaultClient, handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusServiceUnavailable) })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			url := test.url
			if test.handler != nil {
				server := httptest.NewServer(test.handler)
				defer server.Close()
				url = server.URL
			}
			if err := check(test.parent, test.client, url); err == nil {
				t.Fatal("invalid or unhealthy endpoint was accepted")
			} else if !errors.Is(err, errUnhealthy) {
				t.Fatalf("error = %v, want errUnhealthy", err)
			}
		})
	}
}

func TestRunAcceptsExplicitHealthyURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/readyz" {
			t.Fatalf("path = %s, want /readyz", request.URL.Path)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	var stderr bytes.Buffer
	if err := run([]string{server.URL + "/readyz"}, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunReportsUnhealthyEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	var stderr bytes.Buffer
	err := run([]string{server.URL}, &stderr)
	if !errors.Is(err, errUnhealthy) {
		t.Fatalf("error = %v, want errUnhealthy", err)
	}
	if got, want := stderr.String(), "health check failed\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

func TestCheckRejectsMalformedURL(t *testing.T) {
	err := check(context.Background(), http.DefaultClient, "://invalid")
	if !errors.Is(err, errUnhealthy) {
		t.Fatalf("error = %v, want errUnhealthy", err)
	}
}
