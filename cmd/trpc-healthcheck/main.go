// Package main provides the small static health-check binary used by the
// service image. Keeping the probe outside the service command lets the
// runtime image remain distroless while Docker and Compose still get an
// in-container health check.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	defaultHealthURL = "http://127.0.0.1:8080/healthz"
	healthTimeout    = 2 * time.Second
)

var errUnhealthy = errors.New("health check failed")
var exitProcess = os.Exit

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		exitProcess(1)
	}
}

func run(args []string, stderr io.Writer) error {
	url := defaultHealthURL
	if len(args) > 0 {
		url = args[0]
	}
	if err := check(context.Background(), http.DefaultClient, url); err != nil {
		if stderr != nil {
			_, _ = fmt.Fprintln(stderr, err)
		}
		return err
	}
	return nil
}

func check(parent context.Context, client *http.Client, url string) error {
	if parent == nil || client == nil || url == "" {
		return errUnhealthy
	}
	ctx, cancel := context.WithTimeout(parent, healthTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errUnhealthy
	}
	response, err := client.Do(request)
	if err != nil {
		return errUnhealthy
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return errUnhealthy
	}
	return nil
}
