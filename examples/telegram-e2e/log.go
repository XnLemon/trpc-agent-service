package main

import (
	"io"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap/zapcore"
)

const logPrefix = "[telegram-e2e]"

var packageLog = servicelog.NewPrefixedLogger(logPrefix)

func configureLogger(output io.Writer) (func(), error) {
	logger, err := servicelog.New(servicelog.Config{
		Level:       zapcore.InfoLevel,
		Encoding:    servicelog.EncodingConsole,
		Output:      output,
		ErrorOutput: output,
	})
	if err != nil {
		return nil, err
	}
	return servicelog.SetDefault(logger), nil
}
