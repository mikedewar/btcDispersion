package main

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/lovoo/goka"
)

func NewOutboundBTC() *goka.GroupGraph {

	return goka.DefineGroup("outboundBTC",
		goka.Input("BTC", new(txnCodec), keyByOutboundProcessor),
		goka.Output("outboundBTC", new(txnCodec)),
	)
}

func keyByOutboundProcessor(ctx goka.Context, msg interface{}) {
	t := time.Now()
	txn, ok := msg.(Txn)
	if !ok {
		ctx.Fail(fmt.Errorf("couldn't convert value to transaction"))
	}
	// TODO emit once per input
	key := txn.X.Inputs[0].PrevOut.Addr
	ctx.Emit("outboundBTC", key, txn)
	log.WithFields(log.Fields{"elapsed": time.Since(t), "processor": "keyByOutbound"}).Info("keyByOutbound complete")

}
