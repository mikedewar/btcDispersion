package main

import (
	"encoding/json"
	"fmt"

	"github.com/lovoo/goka"
)

func NewOutboundStats() *goka.GroupGraph {

	return goka.DefineGroup("outboundStats",
		goka.Input("windowState-table", new(windowCodec), outboundStatsProcessor),
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

type Stats struct {
	OutboundDegree int
}

type statsCodec struct{}

func (c *statsCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *statsCodec) Decode(data []byte) (interface{}, error) {
	var v Stats
	err := json.Unmarshal(data, &v)
	return v, err
}
