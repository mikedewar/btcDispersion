package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/lovoo/goka"
)

var (
	brokers = []string{"localhost:19092", "localhost:29092", "localhost:39092"}
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	tmgr, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), goka.NewTopicManagerConfig())
	if err != nil {
		log.Fatalf("error creating topic manager: %v", err)
	}

	// make sure the BTC topic is up and running
	err = tmgr.EnsureStreamExists("BTC", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}

	// make sure the outboundBTC topic is up and running
	err = tmgr.EnsureStreamExists("outboundBTC", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}

	// make sure the outboundBTCStats topic is up and running
	err = tmgr.EnsureStreamExists("outboundBTCStats", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}

	// runBTCCollector talks to the blockchain.info websocket and populates the
	// BTC topic in kafka. This simulates a topic we wouldn't have access to and
	// se we don't pretend to control the key
	go runBTCCollector(ctx)

	// outboundGraph simply keys messages by the sending address
	outboundGraph := NewOutboundBTC()
	p0, err := goka.NewProcessor(brokers, outboundGraph)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		err = p0.Run(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// windowGraph builds an array of transactions by address
	windowGraph := NewWindowState()
	p1, err := goka.NewProcessor(brokers, windowGraph)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := p1.Run(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// outboundStatsGraph emits aggregate statistics of each window
	outboundStatsGraph := NewOutboundStats()
	p2, err := goka.NewProcessor(brokers, outboundStatsGraph)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := p2.Run(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	log.Println("sending cancel notification")
	cancel()
}
