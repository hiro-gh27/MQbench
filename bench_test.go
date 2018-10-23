package main

import (
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

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

/*
func TestSingleData(t *testing.T) {
	var pubData []time.Time
	pubData = append(pubData, time.Now())

	var data []pubsubTimeStamp
	ps := pubsubTimeStamp{}
	ps.topic = "hoge"
	ps.published = pubData[0]
	ps.subscribed = time.Now()
	data = append(data, ps)

	res := eliminate(pubData, data)

	expected := 1
	actual := len(res)

	if expected == actual {
		t.Fatalf("miss expected:%d, actual%d", expected, actual)
	}
}
*/

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
