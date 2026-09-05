package main

import (
	"context"
	collector "github.com/Makarechi/firebirdsql-otel/trace"
	"os"
	"os/signal"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "--worker" {
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := collector.RunWorker(ctx, os.Stdin, os.Stdout); err != nil {
		os.Exit(1)
	}
}
