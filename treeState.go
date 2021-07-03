package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/lovoo/goka"
)

type WindowState struct {
	g *goka.GroupGraph
}

func NewWindowState() *WindowState {

	return &WindowState{
		goka.DefineGroup("windowState",
			goka.Input("BTC", new(txnCodec), windowStateProcessor),
			goka.Persist(new(treeCodec)),
		),
	}
}

func (w *WindowState) Run(ctx context.Context, brokers []string) {
	p, err := goka.NewProcessor(brokers, w.g)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("shut down nicely")
}

func windowStateProcessor(ctx goka.Context, msg interface{}) {
	// get the
	treeI := ctx.Value()
	tree := treeI.(Tree)
	tree.addTxn(msg)
	ctx.SetValue(msg)
}

type Tree struct {
	txns []Txn
}

func (tree *Tree) addTxn(txn Txn) {
	txns := tree.txns
	tree.txns = append(txns, txn)
}

type treeCodec struct{}

func (c *treeCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *treeCodec) Decode(data []byte) (interface{}, error) {
	var v Tree
	err := json.Unmarshal(data, &v)
	return v, err
}
