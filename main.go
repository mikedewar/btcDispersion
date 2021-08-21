package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"

	"github.com/lovoo/goka"
)

var (
	brokers = []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"}
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	b := &backoff.Backoff{}

	var tmgr goka.TopicManager
	var err error

	for {
		tmgr, err = goka.NewTopicManager(brokers, goka.DefaultConfig(), goka.NewTopicManagerConfig())
		if err != nil {
			d := b.Duration()
			log.Info("error creating topic manager:", err, ". Reconnecting in ", d)
			time.Sleep(d)
		} else {
			break
		}
	}

	// make sure the BTC topic is up and running
	err = tmgr.EnsureStreamExists("BTC", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}
	log.Println("topic BTC is up and running")

	// make sure the outboundBTC topic is up and running
	err = tmgr.EnsureStreamExists("outboundBTC", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}
	log.Println("topic outboundBTC is up and running")

	// make sure the outboundBTCStats topic is up and running
	err = tmgr.EnsureStreamExists("outboundBTCStats", 10)
	if err != nil {
		log.Fatalf("Error creating sessions: %v", err)
	}
	log.Println("topic outboundBTCStats is up and running")

	// runBTCCollector talks to the blockchain.info websocket and populates the
	// BTC topic in kafka. This simulates a topic we wouldn't have access to and
	// se we don't pretend to control the key
	go runBTCCollector(ctx)
	log.Println("blockchain info websocket reader has been started")

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
	log.Println("outboundBTC processor has been started")

	// windowGraph builds an array of transactions by address
	windowGraph := NewWindows()
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
	log.Println("ountbound windows processor has been started")

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

	log.Println("outbound stats processor has been started")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	log.Println("sending cancel notification")
	cancel()
}
