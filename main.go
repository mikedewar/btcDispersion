package main

import (
	"context"
	"log"
	"os"
	"os/signal"
)

var (
	brokers = []string{"localhost:9092"}
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go runBTCCollector(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	log.Println("sending cancel notification")
	cancel()
}
