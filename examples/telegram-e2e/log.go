package main

import servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"

const logPrefix = "[telegram-e2e]"

var packageLog = servicelog.NewPrefixedLogger(logPrefix)
