package main

import (
	"log"

	"github.com/montanaflynn/stats"
)

func ValueFeatures(w []Txn) []float64 {

	var sumValue float64 // total value
	var meanValue float64
	var medianValue float64

	var err error

	values := make([]float64, len(w)) // the value of each transaction

	for i, txn := range w {
		values[i] = float64(txn.X.Out[0].Value)
	}

	sumValue, err = stats.Sum(values)
	if err != nil {
		log.Fatal(err)
	}

	meanValue, err = stats.Mean(values)
	if err != nil {
		log.Fatal(err)
	}

	medianValue, err = stats.Median(values)
	if err != nil {
		log.Fatal(err)
	}

	features := make([]float64, 3)

	features[0] = sumValue
	features[1] = meanValue
	features[2] = medianValue

	return features

}
