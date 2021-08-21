package main

import (
	"encoding/json"
	"log"

	"github.com/montanaflynn/stats"
)

type Features struct {
	sumValue    float64
	meanValue   float64
	medianValue float64
	degree      float64
}

type featuresCodec struct{}

func (c *featuresCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *featuresCodec) Decode(data []byte) (interface{}, error) {
	var v Features
	err := json.Unmarshal(data, &v)
	return v, err
}

func CalcFeatures(w []Txn) *Features {

	features := new(Features)

	var err error

	values := make([]float64, len(w)) // the value of each transaction

	for i, txn := range w {
		values[i] = float64(txn.X.Out[0].Value)
	}

	features.sumValue, err = stats.Sum(values)
	if err != nil {
		log.Fatal(err)
	}

	features.meanValue, err = stats.Mean(values)
	if err != nil {
		log.Fatal(err)
	}

	features.medianValue, err = stats.Median(values)
	if err != nil {
		log.Fatal(err)
	}

	return features

}
