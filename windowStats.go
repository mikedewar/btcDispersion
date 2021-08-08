package main

import (
	"fmt"

	"github.com/lovoo/goka"
)

func NewOutboundStats() *goka.GroupGraph {

	return goka.DefineGroup("outboundStats",
		goka.Input("windows-table", new(windowCodec), outboundStatsProcessor),
		goka.Output("outboundBTCStats", new(statsCodec)),
	)
}

func outboundStatsProcessor(ctx goka.Context, msg interface{}) {
	window, ok := msg.(Window)
	if !ok {
		ctx.Fail(fmt.Errorf("couldn't convert value to Window"))
	}
	stats := Stats{
		OutboundDegree: len(window.Txns),
	}
	// emit new statistics without changing the key
	ctx.Emit("outboundBTCStats", ctx.Key(), stats)

}
