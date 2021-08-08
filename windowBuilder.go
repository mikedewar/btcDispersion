package main

import (
	"github.com/lovoo/goka"
)

func NewWindows() *goka.GroupGraph {

	return goka.DefineGroup("windows",
		goka.Input("outboundBTC", new(txnCodec), windowsProcessor),
		goka.Persist(new(windowCodec)),
	)
}

// windowStateProcessor relies on the key of the message corresponding to the
// source address of the transaction
func windowsProcessor(ctx goka.Context, msg interface{}) {

	txn := msg.(Txn)

	// retrieve the window from the state by key (source address)
	windowI := ctx.Value()

	var window Window

	// if it's empty, create a new one
	window, ok := windowI.(Window)
	if !ok {
		newTxnWindow := make([]Txn, 0)
		window.Txns = newTxnWindow
	}
	// add the new transaction to it
	window.addTxn(txn)

	// emit the updated window
	ctx.SetValue(window)
}
