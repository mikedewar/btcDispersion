
btcDispersion is a small service designed to explore lovoo's Goka library. 
It builds windows of BTC transactions grouped by sending address and then 
creates a stream of aggregated statistics about those windows. 

# Getting Started

run `docker compose up` to start kafka with three brokers, zookeeper, and
kafdrop.

run `go build && ./btcDispersion` to run the websocket client and the data
processors.

visit kafdrop at `localhost:9000` to see the topics and data
