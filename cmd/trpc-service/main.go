package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice"
	"github.com/XnLemon/trpc-agent-service/trpcservice/bootstrap"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

const (
	defaultListenAddress     = "127.0.0.1:8080"
	defaultShutdownTimeout   = 10 * time.Second
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 30 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 60 * time.Second
)

type serviceOptions struct {
	address         string
	shutdownTimeout time.Duration
}

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := runMain(context.Background(), os.Args[1:], os.Stdout, os.Stderr, signals); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runMain(ctx context.Context, args []string, stdout, stderr io.Writer, signals <-chan os.Signal) error {
	options, help, err := parseServiceOptions(args, stderr)
	if err != nil {
		return err
	}
	if help {
		_, _ = fmt.Fprintf(stdout, "trpc-agent-service %s\nusage: trpc-service [-addr address] [-shutdown-timeout duration]\n", trpcservice.Version)
		return nil
	}
	bootstrapRuntime, err := bootstrap.NewUnavailable()
	if err != nil {
		return err
	}
	handler := bootstrapRuntime.HandlerValue()
	server := newServiceHTTPServer(handler.Handler(), options)
	_, _ = fmt.Fprintf(stdout, "trpc-agent-service %s listening on %s\n", trpcservice.Version, options.address)
	returnErr := runService(ctx, signals, handler, options.shutdownTimeout, server.ListenAndServe, server.Shutdown)
	return errors.Join(returnErr, bootstrapRuntime.Close())
}

func parseServiceOptions(args []string, stderr io.Writer) (serviceOptions, bool, error) {
	options := serviceOptions{address: defaultListenAddress, shutdownTimeout: defaultShutdownTimeout}
	flags := flag.NewFlagSet("trpc-service", flag.ContinueOnError)
	flags.SetOutput(stderr)
	help := flags.Bool("help", false, "show help")
	flags.BoolVar(help, "h", false, "show help")
	flags.StringVar(&options.address, "addr", options.address, "HTTP listen address")
	flags.DurationVar(&options.shutdownTimeout, "shutdown-timeout", options.shutdownTimeout, "graceful shutdown timeout")
	if err := flags.Parse(args); err != nil {
		return serviceOptions{}, false, err
	}
	if flags.NArg() != 0 {
		return serviceOptions{}, false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if options.address == "" {
		return serviceOptions{}, false, fmt.Errorf("listen address cannot be empty")
	}
	if options.shutdownTimeout <= 0 {
		return serviceOptions{}, false, fmt.Errorf("shutdown timeout must be positive")
	}
	return options, *help, nil
}

func newServiceHTTPServer(handler http.Handler, options serviceOptions) *http.Server {
	return &http.Server{
		Addr:              options.address,
		Handler:           handler,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}
}

func runService(ctx context.Context, signals <-chan os.Signal, handler *gateway.HTTPHandler, shutdownTimeout time.Duration, serve func() error, shutdown func(context.Context) error) error {
	if ctx == nil || serve == nil || shutdown == nil || shutdownTimeout <= 0 {
		return fmt.Errorf("invalid service supervisor configuration")
	}
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- serve()
	}()
	select {
	case err := <-serveResult:
		closeErr := closeGatewayHandler(handler)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return closeErr
		}
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	case <-ctx.Done():
		return shutdownService(handler, shutdownTimeout, shutdown)
	case <-signals:
		return shutdownService(handler, shutdownTimeout, shutdown)
	}
}

func shutdownService(handler *gateway.HTTPHandler, timeout time.Duration, shutdown func(context.Context) error) error {
	if handler != nil {
		handler.BeginShutdown()
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := shutdown(shutdownContext)
	return errors.Join(shutdownErr, closeGatewayHandler(handler))
}

func closeGatewayHandler(handler *gateway.HTTPHandler) error {
	if handler == nil {
		return nil
	}
	return handler.Close()
}
