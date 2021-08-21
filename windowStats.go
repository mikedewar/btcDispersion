package main

import (
	"fmt"
	"time"

	"github.com/lovoo/goka"
	log "github.com/sirupsen/logrus"
)

func NewOutboundStats() *goka.GroupGraph {

	return goka.DefineGroup("outboundStats",
		goka.Input("windows-table", new(windowCodec), outboundStatsProcessor),
		goka.Output("outboundBTCStats", new(featuresCodec)),
	)
}

func outboundStatsProcessor(ctx goka.Context, msg interface{}) {

	t := time.Now()
	window, ok := msg.(Window)
	if !ok {
		ctx.Fail(fmt.Errorf("couldn't convert value to Window"))
	}

	features := CalcFeatures(window.Txns)

	// emit new statistics without changing the key
	ctx.Emit("outboundBTCStats", ctx.Key(), features)

	log.WithFields(log.Fields{"elapsed": time.Since(t), "processor": "outboundStats"}).Info("outboundStats complete")

}
