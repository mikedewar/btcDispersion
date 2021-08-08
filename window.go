package main

import "encoding/json"

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
