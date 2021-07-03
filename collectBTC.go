package main

import (
	"context"
	"encoding/json"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
	"github.com/lovoo/goka"
)

func runBTCCollector(ctx context.Context) {

	u, _ := url.Parse("wss://ws.blockchain.info/inv")
	log.Println(u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

	if err != nil {
		log.Fatal(err)
	}

	hi := []byte("{\"op\":\"unconfirmed_sub\"}")

	defer c.Close()

	log.Println(string(hi))
	err = c.WriteMessage(websocket.TextMessage, hi)
	if err != nil {
		log.Println(err)
	}
	log.Println("subscribed")

	emitter, err := goka.NewEmitter(brokers, "BTC", new(txnCodec))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()

	txnChan := make(chan Txn)
	var txn Txn

	for {
		go func() {
			var msg Txn
			msg = make(map[string]interface{})
			err := c.ReadJSON(&msg)
			if err != nil {
				log.Fatal(err)
			}
			txnChan <- msg
		}()

		select {
		case <-ctx.Done():
			log.Println("shutting down cleanly")
			return
		case txn = <-txnChan:
			err = emitter.EmitSync("", txn)
			if err != nil {
				log.Fatalf("error emitting message: %v", err)
			}
		}

	}
}

type Txn map[string]interface{}

type txnCodec struct{}

func (c *txnCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *txnCodec) Decode(data []byte) (interface{}, error) {
	var v Txn
	err := json.Unmarshal(data, &v)
	return v, err
}
