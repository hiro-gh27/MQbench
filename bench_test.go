package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang"

	"go.uber.org/zap"
)

var (
	testPublishers  []mqtt.Client
	testSubscribers []mqtt.Client
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

func TestMain(m *testing.M) {
	before()
	code := m.Run()
	after()
	os.Exit(code)
}

func before() {
	initLogger()
}

func after() {
	if len(testPublishers) != 0 {
		disconnectALL(testPublishers)
	}
	if len(testSubscribers) != 0 {
		disconnectALL(testSubscribers)
	}
}

func createTestClientsToLocalBroker(pNum int, sNum int) {
	testPublishers = newConnectedClients("p", "tcp://127.0.0.1:1883", pNum)
	testSubscribers = newConnectedClients("s", "tcp://127.0.0.1:1883", sNum)
}

//ipアドレスをあとでちゃんと埋める．
func createTestClientsToRemoteBroker(pNum int, sNum int) {
	testPublishers = newConnectedClients("p", "tcp://", pNum)
	testSubscribers = newConnectedClients("s", "tcp://", sNum)
}

//まだ実装途中で止まっている．
func TestSinglePubSub(t *testing.T) {
	createTestClientsToLocalBroker(1, 1)
	terminateSubSync := sync.WaitGroup{}
	terminateSubSync.Add(1)
	setSubscriber(testSubscribers, &terminateSubSync)
}

func TestSinglePublishForTimestamp(t *testing.T) {
	createTestClientsToLocalBroker(1, 0)
	topic := "hoge"
	qos := (byte)(0)
	retain := false
	msg := ""
	publishedTimestamp := time.Now()
	msgPadding := getMessage(1024 - len(stampMQTT) - 1)
	msg = publishedTimestamp.Format(stampMQTT) + "/" + msgPadding
	pubToken := testPublishers[0].Publish(topic, qos, retain, msg)
	if pubToken.Wait() && pubToken.Error() != nil {
		t.Errorf("can't published")
	}
}

func TestMultiData(t *testing.T) {
	expected := 10
	var pubData []time.Time
	for index := 0; index < expected; index++ {
		pubData = append(pubData, time.Now())
	}

	var inputData []pubsubTimeStamp
	for index := 0; index < expected*3; index++ {
		var p time.Time
		if index >= expected && index < expected*2 {
			p = pubData[index-expected]
		} else {
			p = time.Now()
		}
		input := pubsubTimeStamp{}
		input.published = p
		input.subscribed = time.Now()
		inputData = append(inputData, input)
	}

	res := eliminate(pubData, inputData)

	actual := len(res.ingressData)
	if expected != actual {
		/*
			for index := 0; index < len(res); index++ {
				id := res[index]
				fmt.Printf("\nindex:%d published:%s, subscribed:%s", index, id.published, id.subscribed)
			}
		*/
		t.Fatalf("miss expected:%d, actual:%d", expected, actual)
	}
}

func TestAmountData(t *testing.T) {
	expected := 100000
	var pub []time.Time
	var input []pubsubTimeStamp

	//テストデータの準備
	for index := 0; index < expected; index++ {
		pub = append(pub, time.Now())
	}
	for index := 0; index < expected*3; index++ {
		p := time.Now()
		time.Sleep(time.Microsecond * 1)
		s := time.Now()
		i := pubsubTimeStamp{published: p, subscribed: s}
		input = append(input, i)
		if index >= expected && index < expected*2 {
			pub = append(pub, p)
		}
	}
	for index := 0; index < expected; index++ {
		pub = append(pub, time.Now())
	}

	for index := 0; index < len(pub); index++ {
		//fmt.Printf("%s\n", pub[index])
	}
	for index := 0; index < len(input); index++ {
		//fmt.Printf("%s\n", input[index].published)
	}
	actual := len(eliminate(pub, input).ingressData)
	if actual != expected {
		t.Fatalf("expected=%d, actual=%d", expected, actual)
	}
}
