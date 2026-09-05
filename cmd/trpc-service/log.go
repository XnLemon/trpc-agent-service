package main

import servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"

const logPrefix = "[trpc-service]"

var packageLog = servicelog.NewPrefixedLogger(logPrefix)
