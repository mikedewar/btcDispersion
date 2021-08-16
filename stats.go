package main

import "encoding/json"

type Stats struct {
	OutboundDegree int
	ValueFeatures  []float64
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
