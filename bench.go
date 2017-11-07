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
	logger       *zap.Logger
	qos          byte
	retain       bool
	topic        string
	size         int
	baseMSG      string
	load         float64
	config       string
	clientNum    = 0
	publishers   []mqtt.Client
	subscribers  []mqtt.Client
	timeLocation *time.Location
)

// 評価に使う時間
var (
	evaluateStartTime time.Time

	warmUp     = time.Second * 1
	production = time.Second * 60
	coolDown   = time.Second * 1
)

const (
	letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
	tsLayout      = time.StampNano + " 2006"
	stampMQTT     = "2006-01-02T15:04:05.000000000Z07:00"
)

type pubsubTimeStamp struct {
	topic      string
	published  time.Time
	subscribed time.Time
}

// 構造体のソートを定義する
type timeSort []time.Time
type pubSort []pubsubTimeStamp

func (x timeSort) Len() int { return len(x) }
func (x timeSort) Less(i, j int) bool {
	itime := x[i]
	jtime := x[j]
	dtime := jtime.Sub(itime)
	return dtime > 0
}
func (x timeSort) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x pubSort) Len() int { return len(x) }
func (x pubSort) Less(i, j int) bool {
	itime := x[i].published
	jtime := x[j].published
	dtime := jtime.Sub(itime)
	return dtime > 0
}
func (x pubSort) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func main() {
	fmt.Println("hello world")

	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	timeLocation, _ = time.LoadLocation("Asia/Tokyo")

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

	/*
		ここから実行メソッド
	*/
	lancher()
	wg := sync.WaitGroup{}
	wg.Add(1)

	endLock := sync.WaitGroup{}
	endLock.Add(1)
	var badSubscribeData []pubsubTimeStamp
	go func() {
		defer wg.Done()
		badSubscribeData = setSubscriber(subscribers, &endLock)
	}()

	fmt.Println("実行待ち")
	time.Sleep(5 * time.Second)
	publishData := execute(publishers)
	time.Sleep(5 * time.Second)
	endLock.Done()
	wg.Wait()
	fmt.Println("実行終了")

	var evaluateData []pubsubTimeStamp
	start := evaluateStartTime.Add(warmUp)
	finish := start.Add(production)
	pubCount := 0

	//setsubscriberからの入手データと比較を行って，該当部分の正しいデータを取得する．
	var subscribeData []pubsubTimeStamp
	fmt.Printf("check good subscribe data START, len(badSubscribeData)=%d\n", len(badSubscribeData))

	//ここで範囲外の部分が切り捨てられている可能性があるためチェックしてる
	//-> publish時刻でジャッジしていたために，特に悪い問題は起こしていないと思いわれ...
	for _, bsd := range badSubscribeData {
		if bsd.published.Sub(start) > 0 && bsd.published.Sub(finish) < 0 {
			subscribeData = append(subscribeData, bsd)
		}
	}
	fmt.Printf("check good subscribe data END, len(subscribeData)=%d\n", len(subscribeData))

	//subscribeData = badSubscribeData

	sort.Sort(pubSort(subscribeData))
	fmt.Printf("execute start time is: %s\n", evaluateStartTime)
	// 複数該当時に漏れが発生するために要注意

	totalCount := 0
	elseCount := 0

	for outer := 0; outer < len(publishData); outer++ {
		totalCount++
		pd := publishData[outer]
		if pd.Sub(start) > 0 && pd.Sub(finish) < 0 {
			pubCount++
			for inner := 0; inner < len(subscribeData); inner++ {
				sd := subscribeData[inner]

				if pd.Sub(sd.published) > 0 {
					continue
				} else if pd.Sub(sd.published) == 0 {
					s := time.Now()
					evaluateData = append(evaluateData, sd)
					m := time.Now()
					if inner > 0 {
						subscribeData = append(subscribeData[:inner], subscribeData[inner+1:]...)
					} else {
						subscribeData = subscribeData[inner+1:]
					}
					e := time.Now()
					logger.Debug(fmt.Sprintf("start->midele:%s, middle->end:%s, evaluateData size:%d\n ", m.Sub(s), e.Sub(m), len(subscribeData)))
				} else if pd.Sub(sd.published) < 0 {
					break
				}
			}
		} else {
			elseCount++
		}
	}
	fmt.Printf("total:%d, pubCount:%d, elseCount:%d\n", totalCount, pubCount, elseCount)

	/**
	* 評価の計算式をここから
	 */
	var ePubStamp []time.Time
	var eSubStamp []time.Time
	for _, ed := range evaluateData {
		ePubStamp = append(ePubStamp, ed.published)
		eSubStamp = append(eSubStamp, ed.subscribed)
	}

	sort.Sort(timeSort(ePubStamp))
	sort.Sort(timeSort(eSubStamp))

	var pTotalDuration time.Duration
	var pMillsecondDuration float64
	var pThroughput float64
	fmt.Printf("len(ePubStamps):%d\n", len(ePubStamp))
	fmt.Printf("first:%s\n", ePubStamp[0])
	fmt.Printf("last:%s\n", ePubStamp[len(ePubStamp)-1])
	pTotalDuration = ePubStamp[len(ePubStamp)-1].Sub(ePubStamp[0])
	pMillsecondDuration = float64(pTotalDuration.Nanoseconds()) / math.Pow10(6)
	pThroughput = float64(len(ePubStamp)) / pMillsecondDuration
	fmt.Printf("pub thoughput: %fmsg/ms\n", pThroughput)

	var sTotalDuration time.Duration
	var sMillsecondDuration float64
	var sThroughput float64
	fmt.Printf("len(eSubStamps):%d\n", len(eSubStamp))
	sTotalDuration = eSubStamp[len(eSubStamp)-1].Sub(eSubStamp[0])
	sMillsecondDuration = float64(sTotalDuration.Nanoseconds()) / math.Pow10(6)
	sThroughput = float64(len(eSubStamp)) / sMillsecondDuration
	fmt.Printf("sub thoughput: %fmsg/ms\n", sThroughput)

	lostNum := float64(pubCount - len(evaluateData))
	fmt.Printf("total msgNum:%d, lost num:%f\n", pubCount, lostNum)
	fmt.Printf("lost rate: %f%%\n", lostNum/float64(len(publishData))*100)
	/** ここまで **/

	fmt.Printf("first entry, publish:%s, subscribe:%s\n", evaluateData[0].published, evaluateData[0].subscribed)
	fmt.Printf("last entry, publish:%s, subscribe:%s\n", evaluateData[len(evaluateData)-1].published, evaluateData[len(evaluateData)-1].subscribed)

	disconnectALL(publishers)
	disconnectALL(subscribers)
}

func lancher() {
	qosFlag := flag.Int("qos", 0, "MQTT QoS(0|1|2)")
	retainFlag := flag.Bool("retain", false, "MQTT Retain")
	topicFlag := flag.String("topic", "", "Base topic")
	sizeFlag := flag.Int("size", 100, "Message size per publish (byte)")
	loadFlag := flag.Float64("load", 5, "publish/ms")
	configFlag := flag.String("file", "NONE", "Base file name")
	flag.Parse()

	qos = byte(*qosFlag)
	topic = *topicFlag
	size = *sizeFlag
	load = *loadFlag
	config = *configFlag
	retain = *retainFlag
	logger.Debug(fmt.Sprintf("qos: %d, retain: %t, topic: %s, size: %d, load: %f",
		qos, retain, topic, size, load))

	publishers = newConnectedClients("tcp://10.0.0.2:1883", 10)
	subscribers = newConnectedClients("tcp://10.0.0.2:1883", 10)

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

func setSubscriber(subscribers []mqtt.Client, endLock *sync.WaitGroup) []pubsubTimeStamp {
	rStack := make([]*[]pubsubTimeStamp, len(subscribers))
	for index := 0; index < len(subscribers); index++ {
		id := index
		s := subscribers[id]
		topic := fmt.Sprintf("%05d", id)
		rVal := []pubsubTimeStamp{}
		rStack[id] = &rVal

		var callback mqtt.MessageHandler = func(c mqtt.Client, msg mqtt.Message) {
			var psts pubsubTimeStamp
			sst := time.Now().Format(stampMQTT)
			st, _ := time.Parse(stampMQTT, sst)
			spt := string(msg.Payload()[:35])
			pt, _ := time.Parse(stampMQTT, spt)

			psts.topic = msg.Topic()
			psts.published = pt
			psts.subscribed = st
			rVal = append(rVal, psts)
			logger.Debug(fmt.Sprintf("topic:%s pub:%s, sub%s\n", psts.topic, psts.published, psts.subscribed))
		}
		token := s.Subscribe(topic, qos, callback)
		if token.Wait() && token.Error() != nil {
			fmt.Printf("Subscribe Error: %s\n", token.Error())
		}
	}

	var rvals []pubsubTimeStamp
	fmt.Println("wait start in setSubscribers")
	endLock.Wait()
	fmt.Println("wait end in setSubscribers")
	//time.Sleep(time.Second*20 + warmUp + production + coolDown)
	for index := 0; index < len(subscribers); index++ {
		rs := *rStack[index]
		fmt.Printf("len(rvs)=%d\n", len(rs))
		for _, val := range rs {

			rvals = append(rvals, val)

			//fmt.Printf("topic:%s pub:%s, sub%s\n", val.topic, val.published, val.subscribed)
		}
	}
	return rvals
}

func execute(publishers []mqtt.Client) []time.Time {
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
					logger.Debug(fmt.Sprintf("gap=%s, ideal=%s\n", gap, ideal))
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

				if timeStamp.Sub(startTS) > warmUp+production+coolDown {
					break
				}
			}
			timeCh <- pts
		}(index)
	}

	time.Sleep(time.Second * 10)
	evaluateStartTime = time.Now()
	redy.Done()

	for index := 0; index < len(publishers); index++ {
		allPublishedTimeStamp = append(allPublishedTimeStamp, <-timeCh...)
	}

	sort.Sort(timeSort(allPublishedTimeStamp))

	var rData []time.Time

	fmt.Printf("len(allPublishedTimeStamp:%d\n", len(allPublishedTimeStamp))
	for index := 0; index < len(allPublishedTimeStamp); index++ {
		str := allPublishedTimeStamp[index].Format(stampMQTT)
		st, _ := time.Parse(stampMQTT, str)
		rData = append(rData, st)
	}
	total := allPublishedTimeStamp[len(allPublishedTimeStamp)-1].Sub(allPublishedTimeStamp[0])
	millDuration := float64(total.Nanoseconds()) / math.Pow10(6)
	th := float64(len(allPublishedTimeStamp)) / millDuration
	fmt.Printf("thoughput: %fmsg/ms\n", th)

	//return allPublishedTimeStamp
	return rData
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
