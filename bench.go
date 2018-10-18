package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
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

	warmUp     = time.Second * 10
	production = time.Second * 600
	coolDown   = time.Second * 10

	exportFile string
)

const (
	letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
	tsLayout      = time.StampNano + " 2006"
	stampMQTT     = "2006-01-02T15:04:05.000000000Z07:00"
)

type broker struct {
	Type   string `json:"type"`
	Host   string `json:"host"`
	PeerID string `json:"peerID"`
	Num    int    `json:"number"`
}

type pubsubTimeStamp struct {
	topic      string
	published  time.Time
	subscribed time.Time
}

// evaData is formatted data
type evaData struct {
	publishData []time.Time
	ingressData []time.Time
	egressData  []time.Time
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
	time.Sleep(20 * time.Second)
	//time.Sleep(1 * time.Hour)
	publishData := execute(publishers)
	time.Sleep(5 * time.Second)
	endLock.Done()
	wg.Wait()
	fmt.Println("実行終了")

	//var evaluateData []pubsubTimeStamp
	start := evaluateStartTime.Add(warmUp)
	finish := start.Add(production)

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

	/** 評価の計算式をここから **/
	data := eliminate(publishData, subscribeData)

	evaluateData := make([]pubsubTimeStamp, len(data.ingressData))
	originPubData := data.publishData
	ePubStamp := data.ingressData
	eSubStamp := data.egressData
	pubCount := len(originPubData)
	totalDurarionNano := int64(0)
	for index := 0; index < len(ePubStamp); index++ {
		evaluateData[index].published = ePubStamp[index]
		evaluateData[index].subscribed = eSubStamp[index]
		totalDurarionNano += eSubStamp[index].Sub(ePubStamp[index]).Nanoseconds()
	}

	fmt.Printf("tRttNanoDuration=%d\n", totalDurarionNano)
	RTT := float64(totalDurarionNano / int64(len(evaluateData)))
	mRTT := RTT / math.Pow10(6)
	if RTT > math.Pow10(9) {
		fmt.Printf("ave RTT:%fs\n", RTT/math.Pow10(9))
	} else if RTT > math.Pow10(6) {
		fmt.Printf("ave RTT:%fms\n", RTT/math.Pow10(6))
	} else if RTT > math.Pow10(3) {
		fmt.Printf("ave RTT:%fμs\n", RTT/math.Pow10(3))
	} else {
		fmt.Printf("ave RTT:%fns\n", RTT)
	}
	sort.Sort(timeSort(ePubStamp))
	sort.Sort(timeSort(eSubStamp))

	//各種取得データの統計
	var opTotalDuration time.Duration
	var opMillsecondDuration float64
	var opTroughput float64
	opTotalDuration = originPubData[len(originPubData)-1].Sub(originPubData[0])
	opMillsecondDuration = float64(opTotalDuration.Nanoseconds()) / math.Pow10(6)
	opTroughput = float64(len(originPubData)) / opMillsecondDuration
	fmt.Printf("pub thoughput: %fmsg/ms\n", opTroughput)

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
	lostRatio := lostNum / float64(len(publishData)) * 100
	fmt.Printf("total msgNum:%d, lost num:%f\n", pubCount, lostNum)
	fmt.Printf("lost rate: %f%%\n", lostRatio)

	//性能限界には，評価ツールからのpublish[ms]と計測データからのsubscribe[msg/ms]を用いる
	//ratio := (sThroughput / pThroughput) * 100
	ratio := (sThroughput / opTroughput) * 100
	fmt.Printf("new Definition1: %f%%\n", ratio)

	/** ここまで **/
	fmt.Printf("first entry, publish:%s, subscribe:%s\n", evaluateData[0].published, evaluateData[0].subscribed)
	fmt.Printf("last entry, publish:%s, subscribe:%s\n", evaluateData[len(evaluateData)-1].published, evaluateData[len(evaluateData)-1].subscribed)

	/**
	* データ出力していきます
	**/
	filePath := fmt.Sprintf("/Users/hiroki/Downloads/%s.csv", exportFile)
	fp, first := newFile(filePath)
	writer := bufio.NewWriter(fp)

	if first {
		_, err := writer.WriteString(fmt.Sprintf("publish[msg/ms], ingress [msg/ms],egress [msg/ms],latecy [ms],total msg,lost msg,Definition [%%],lost rate [%%]\n"))
		if err != nil {
			log.Fatal(err)
		}
	}
	expotData := fmt.Sprintf("%f,%f,%f,%f,%d,%d,%f,%f\n",
		opTroughput, pThroughput, sThroughput, mRTT, pubCount, int(lostNum), ratio, lostRatio)
	_, err = writer.WriteString(expotData)
	if err != nil {
		log.Fatal(err)
	}

	writer.Flush()
	fp.Close()

	disconnectALL(publishers)
	disconnectALL(subscribers)
}
func newFile(fn string) (*os.File, bool) {
	_, exist := os.Stat(fn)
	fp, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return fp, os.IsNotExist(exist)
}

func lancher() {
	qosFlag := flag.Int("qos", 0, "MQTT QoS(0|1|2)")
	retainFlag := flag.Bool("retain", false, "MQTT Retain")
	topicFlag := flag.String("topic", "", "Base topic")
	sizeFlag := flag.Int("size", 100, "Message size per publish (byte)")
	loadFlag := flag.Float64("load", 5, "publish/ms")
	configFlag := flag.String("file", "NONE", "Base file name")
	brokersFlag := flag.String("broker", "NONE", "json")

	flag.Parse()

	qos = byte(*qosFlag)
	topic = *topicFlag
	size = *sizeFlag
	load = *loadFlag
	config = *configFlag
	retain = *retainFlag
	b := *brokersFlag
	exportFile = fmt.Sprintf("%s[load=%f]", b[:len(b)-5], load)
	fmt.Printf("export file name is %s\n", exportFile)
	//exportFile = *exportFileFlag
	logger.Debug(fmt.Sprintf("qos: %d, retain: %t, topic: %s, size: %d, load: %f",
		qos, retain, topic, size, load))

	//brokerに接続するところをjsonで読み込む 2018/02/07
	brokersJSON := fmt.Sprintf("./exp/%s", *brokersFlag)
	byteS, err := ioutil.ReadFile(brokersJSON)
	if err != nil {
		log.Fatal(err)
	}
	var brokers []broker
	if err := json.Unmarshal(byteS, &brokers); err != nil {
		log.Fatal(err)
	}
	for _, b := range brokers {
		c := newConnectedClients(b.Type, b.Host, b.Num)
		if b.Type == "pub" {
			publishers = append(publishers, c...)
		} else {
			subscribers = append(subscribers, c...)
		}
	}
}

func newConnectedClients(cType string, broker string, number int) []mqtt.Client {
	var clients []mqtt.Client

	for index := clientNum; index < clientNum+number; index++ {
		if cType == "pub" {
			fmt.Printf("pub connect: %s\n", broker)
		} else if cType == "sub" {
			fmt.Printf("sub connect: %s\n", broker)
		}
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
		//topic := fmt.Sprintf("%05d", 1)
		//topic := fmt.Sprintf("%05d", id*2+1)
		topic := fmt.Sprintf("%05d", id)
		// add for test 12/25
		//topic = fmt.Sprintf("%05d", 0)
		fmt.Printf("sub topic: %s\n", topic)
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
	time.Sleep(5 * time.Second)
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
		//debugに使っただけなので決してよい 11/15 22:50
		time.Sleep(time.Millisecond * 30)
		go func(index int) {
			var timeStamp time.Time
			var pts []time.Time
			p := publishers[index]
			topic := fmt.Sprintf("%05d", index)
			//topic := fmt.Sprintf("%05d", index*2+1)
			//topic = fmt.Sprintf("%05d", 0)
			//topic := fmt.Sprintf("%05d", index+2)
			fmt.Printf("publish topic%s\n", topic)
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

func eliminate(filter []time.Time, data []pubsubTimeStamp) evaData {
	var publish, ingress, egress []time.Time
	sort.Sort(pubSort(data))
	sort.Sort(timeSort(filter))

	in := 0
	start := evaluateStartTime.Add(warmUp)
	end := start.Add(production)
	for out := 0; out < len(filter); out++ {
		f := filter[out]
		if f.Sub(start) > 0 && f.Sub(end) < 0 {
			publish = append(publish, f)
			for in < len(data) {
				d := data[in]
				diration := d.published.Sub(f)
				if diration < 0 {
					in++
				} else if diration == 0 {
					ingress = append(ingress, d.published)
					egress = append(egress, d.subscribed)
					in++
				} else {
					break
				}
			}
		}
	}
	res := evaData{publishData: publish, ingressData: ingress, egressData: egress}
	return res
}
