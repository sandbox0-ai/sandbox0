package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/pkg/nodebootstrap"
)

func main() {
	configFile := flag.String("config", "/etc/sandbox0/node-bootstrap.json", "node bootstrap configuration file")
	bootstrapResponse := flag.String("bootstrap-response", "", "initial manager bootstrap response")
	renew := flag.Bool("renew", false, "renew an already enrolled exact node identity")
	flag.Parse()
	if (*bootstrapResponse == "") == !*renew {
		fatal("exactly one of --bootstrap-response or --renew is required")
	}
	config, err := nodebootstrap.LoadConfig(*configFile)
	if err != nil {
		fatal("load configuration: %v", err)
	}
	bootstrapper, err := nodebootstrap.New(config)
	if err != nil {
		fatal("configure bootstrapper: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *renew {
		err = bootstrapper.Renew(ctx)
	} else {
		err = bootstrapper.Initial(ctx, *bootstrapResponse)
	}
	if err != nil {
		fatal("node bootstrap failed: %v", err)
	}
}

func fatal(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
