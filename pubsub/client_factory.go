package pubsub

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
	"os"
	"strconv"
	"time"
)

type clientFactory struct {
	counter int
}

func NewClientFactory() *clientFactory {
	cf := &clientFactory{
		counter: 0,
	}
	return cf
}

func (cf *clientFactory) GetConnected(cType string, broker string, number int) []mqtt.Client {
	var clients []mqtt.Client

	//logger, _ := zap.NewDevelopment()
	//logger := zap.L()
	for index := cf.counter; index < cf.counter+number; index++ {
		if cType == "pub" {
			logger.Debug("publisher connect to", zap.String("broker", broker))
		} else if cType == "sub" {
			logger.Debug("subscriber connect to", zap.String("broker", broker))
		}
		id := index
		processID := strconv.FormatInt(int64(os.Getpid()), 16)
		clientID := fmt.Sprintf("%s-%d", processID, id)
		//logger.Debug(fmt.Sprintf("broker: %s, clientID %s", broker, clientID))

		opts := mqtt.NewClientOptions()
		opts.AddBroker(broker)
		opts.SetClientID(clientID)
		opts.SetKeepAlive(3000 * time.Second)
		client := mqtt.NewClient(opts)

		token := client.Connect()
		if token.Wait() && token.Error() != nil {
			logger.Error("Connected error:", zap.Error(token.Error()))
			client = nil
		}
		clients = append(clients, client)
	}
	cf.counter += number

	// if can't connected, disconnect all clients
	var goodClients []mqtt.Client
	for _, c := range clients {
		if c != nil {
			goodClients = append(goodClients, c)
		}
	}
	if len(goodClients) < len(clients) {
		println("### Error!! ###")
		DisconnectALL(goodClients)
	}
	return clients
}
