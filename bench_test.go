package main

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"go.uber.org/zap"
)

func initLogger() {
	configJSON, err := ioutil.ReadFile("./config/logging.json")
	if err != nil {
		panic(err)
	}
	var myConfig zap.Config
	if err := json.Unmarshal(configJSON, &myConfig); err != nil {
		panic(err)
	}
	logger, _ = myConfig.Build()
	defer logger.Sync()
}

func TestConnectedClients(t *testing.T) {
	initLogger()
	number := 10
	clients := newConnectedClients("tcp://127.0.0.1:1883", number)
	if len(clients) != number {
		t.Fatalf("connected errro")
	}
}
