package main

import (
	"encoding/json"
	"log"

	"github.com/lovoo/goka"
)

func NewWindowState() *goka.GroupGraph {

	return goka.DefineGroup("windowState",
		goka.Input("outboundBTC", new(txnCodec), windowStateProcessor),
		goka.Persist(new(windowCodec)),
	)
}

// windowStateProcessor relies on the key of the message corresponding to the
// source address of the transaction
func windowStateProcessor(ctx goka.Context, msg interface{}) {

	txn := msg.(Txn)

	// retrieve the window from the state by key (source address)
	windowI := ctx.Value()

	var window Window
	window, ok := windowI.(Window)
	if !ok {
		log.Println("Making new window for address", ctx.Key())
		newTxnWindow := make([]Txn, 0)
		window.Txns = newTxnWindow
	}
	if ok {
		log.Println("Got window for address", ctx.Key())
	}
	// add the new transaction to it
	window.addTxn(txn)

	// emit the updated window
	ctx.SetValue(window)
}

type Window struct {
	Txns []Txn
}

func (tree *Window) addTxn(txn Txn) {
	// TODO turn this into a btree so we don't worry about order
	txns := tree.Txns
	tree.Txns = append(txns, txn)
}

type windowCodec struct{}

func (c *windowCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *windowCodec) Decode(data []byte) (interface{}, error) {
	var v Window
	err := json.Unmarshal(data, &v)
	return v, err
}
