package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

var (
	//base        = "MQbench"
	logger      *zap.Logger
	qos         byte
	retain      bool
	topic       string
	size        int
	baseMSG     string
	load        float64
	config      string
	clientNum   = 0
	publishers  []mqtt.Client
	subscribers []mqtt.Client
)

const (
	letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
	tsLayout      = time.StampNano + " 2006"
	stampMQTT     = "2006-01-02T15:04:05.000000000Z07:00"
)

type timeSort []time.Time

func (x timeSort) Len() int { return len(x) }
func (x timeSort) Less(i, j int) bool {
	itime := x[i]
	jtime := x[j]
	dtime := jtime.Sub(itime)
	return dtime > 0
}
func (x timeSort) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func main() {
	fmt.Println("hello world")

	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())

	//logの初期化
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

	lancher()
	executePublish(publishers)

	disconnectALL(publishers)
	disconnectALL(subscribers)
}

func lancher() {
	qosFlag := flag.Int("qos", 0, "MQTT QoS(0|1|2)")
	retainFlag := flag.Bool("retain", false, "MQTT Retain")
	topicFlag := flag.String("topic", "", "Base topic")
	sizeFlag := flag.Int("size", 100, "Message size per publish (byte)")
	loadFlag := flag.Float64("load", 30, "publish/ms")
	configFlag := flag.String("file", "NONE", "Base file name")
	flag.Parse()

	qos = byte(*qosFlag)
	retain = *retainFlag
	topic = *topicFlag
	size = *sizeFlag
	load = *loadFlag
	config = *configFlag

	logger.Debug(fmt.Sprintf("qos: %d, retain: %t, topic: %s, size: %d, load: %f",
		qos, retain, topic, size, load))

	publishers = newConnectedClients("tcp://127.0.0.1:1883", 10)
	subscribers = newConnectedClients("tcp://127.0.0.1:1883", 10)

}

func newConnectedClients(broker string, number int) []mqtt.Client {
	var clients []mqtt.Client

	for index := clientNum; index < clientNum+number; index++ {
		id := index
		prosessID := strconv.FormatInt(int64(os.Getpid()), 16)
		clientID := fmt.Sprintf("%s-%d", prosessID, id)
		logger.Debug(fmt.Sprintf("broker: %s, clientID %s", broker, clientID))

		opts := mqtt.NewClientOptions()
		opts.AddBroker(broker)
		opts.SetClientID(clientID)
		opts.SetKeepAlive(3000 * time.Second)
		client := mqtt.NewClient(opts)
		token := client.Connect()
		if token.Wait() && token.Error() != nil {
			fmt.Printf("Connected error: %s\n", token.Error())
			client = nil
		}
		clients = append(clients, client)
	}
	clientNum += number

	// if can't connected, disconnect all clients
	var goodClients []mqtt.Client
	for _, c := range clients {
		if c != nil {
			goodClients = append(goodClients, c)
		}
	}
	if len(goodClients) < len(clients) {
		println("### Error!! ###")
		disconnectALL(goodClients)
	}

	return clients
}

func disconnectALL(clinets []mqtt.Client) {
	for _, c := range clinets {
		c.Disconnect(500)
	}
}

func executePublish(publishers []mqtt.Client) {
	//初期化
	var allPublishedTimeStamp []time.Time

	randMsg := getMessage(size - len(stampMQTT) - 1)
	maxInterval := float64(len(publishers)) / load
	timeCh := make(chan []time.Time)
	redy := sync.WaitGroup{}
	redy.Add(1)

	//goroutineでpublishを実行する
	for index := 0; index < len(publishers); index++ {
		go func(index int) {
			var timeStamp time.Time
			var pts []time.Time
			p := publishers[index]
			topic := fmt.Sprintf("%05d", index)
			firstSleepDuration := getRandomInterval(maxInterval)
			logger.Info(fmt.Sprintf("first: %s", firstSleepDuration))

			redy.Wait()
			startTS := time.Now()
			time.Sleep(firstSleepDuration)
			for count := 0; ; count++ {
				if count > 0 {
					gap := time.Now().Sub(startTS)
					ideal := time.Duration(maxInterval * 1000 * 1000 * float64(count))
					wait := ideal - gap
					fmt.Printf("gap=%s, ideal=%s\n", gap, ideal)
					if wait > 0 {
						time.Sleep(wait)
					}
				}
				timeStamp = time.Now()
				msg := timeStamp.Format(stampMQTT) + "/" + randMsg + topic
				logger.Debug(fmt.Sprintf("topic:%s, ts:%s", topic, timeStamp.Format(stampMQTT)))
				//logger.Info(msg)

				token := p.Publish(topic, qos, retain, msg)
				if token.Wait() && token.Error() != nil {
					fmt.Printf("publish error: %s\n", token.Error())
				} else {
					pts = append(pts, timeStamp)
				}

				if timeStamp.Sub(startTS) > time.Second*10 {
					break
				}
			}
			timeCh <- pts
		}(index)
	}

	time.Sleep(time.Second * 3)
	redy.Done()

	for index := 0; index < len(publishers); index++ {
		allPublishedTimeStamp = append(allPublishedTimeStamp, <-timeCh...)
	}

	sort.Sort(timeSort(allPublishedTimeStamp))
	fmt.Printf("len(allPublishedTimeStamp:%d\n", len(allPublishedTimeStamp))
	for index := 0; index < len(allPublishedTimeStamp); index++ {
		fmt.Printf("index:%d, ts:%s\n", index, allPublishedTimeStamp[index])
	}

	total := allPublishedTimeStamp[len(allPublishedTimeStamp)-1].Sub(allPublishedTimeStamp[0])
	millDuration := float64(total.Nanoseconds()) / math.Pow10(6)
	th := float64(len(allPublishedTimeStamp)) / millDuration
	fmt.Printf("thoughput: %fmsg/ms\n", th)
}

func getMessage(strlen int) string {
	if strlen < 0 {
		strlen = 1
	}
	message := make([]byte, strlen)
	cache, remain := rand.Int63(), letterIdxMax
	for i := strlen - 1; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		idx := int(cache & letterIdxMask)
		if idx < len(letters) {
			message[i] = letters[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	logger.Debug(string(message))
	return string(message)
}

func getRandomInterval(max float64) time.Duration {
	var td time.Duration
	nanoMax := int(max * 1000 * 1000)
	if max > 0 {
		interval := rand.Intn(nanoMax)
		td = time.Duration(interval) * time.Nanosecond
	}
	return td
}
