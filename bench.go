package main

import (
	"MQbench/pubsub"
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go.uber.org/zap/zapcore"
	"io/ioutil"

	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

var (
	//base        = "MQbench"
	logger       *zap.Logger
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

	warmUp     = time.Second * 3
	production = time.Second * 3
	coolDown   = time.Second * 3

	exportFile string
)

var (
	configurations *pubsub.Configurations
)

const (
	stampMQTT = "2006-01-02T15:04:05.000000000Z07:00"
)

type broker struct {
	Type   string `json:"type"`
	Host   string `json:"host"`
	PeerID string `json:"peerID"`
	Num    int    `json:"number"`
}

// evaData is formatted data
type evaData struct {
	publishData []time.Time
	ingressData []time.Time
	egressData  []time.Time
}

// 構造体のソートを定義する
type timeSort []time.Time

func (x timeSort) Len() int { return len(x) }
func (x timeSort) Less(i, j int) bool {
	itime := x[i]
	jtime := x[j]
	duration := jtime.Sub(itime)
	return duration > 0
}
func (x timeSort) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

type pubSort []pubsub.PubSubTimeStamp

func (x pubSort) Len() int { return len(x) }
func (x pubSort) Less(i, j int) bool {
	itime := x[i].Published
	jtime := x[j].Published
	dtime := jtime.Sub(itime)
	return dtime > 0
}
func (x pubSort) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func main() {
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
	myConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, _ = myConfig.Build()
	zap.ReplaceGlobals(logger)
	// いい方法考えたいが...
	pubsub.Init()

	defer logger.Sync()

	logger.Info("Hello")
	/*
	   ここから実行メソッド
	*/
	launcher()

	subGroupFinishLook := &sync.WaitGroup{}
	subGroupFinishLook.Add(1)

	sb := pubsub.NewSubscribersGroup(subscribers)
	sbFuture := sb.ExecuteAsync(subGroupFinishLook)

	fmt.Println("実行待ち")
	time.Sleep(20 * time.Second)
	publishData := execute(publishers)

	/*
		ps := pubsub.NewPublishersGroup(*configurations)
		var publishData []time.Time
		d := warmUp + production + coolDown
		publishData, evaluateStartTime := ps.Execute(publishers, d)
	*/

	time.Sleep(5 * time.Second)
	subGroupFinishLook.Done()
	originalSubscribeTimes := <-sbFuture
	//subscribingFuture.Wait()

	fmt.Println("実行終了")

	//var evaluateData []pubsubTimeStamp
	start := evaluateStartTime.Add(warmUp)
	end := start.Add(production)
	targetSubscribeData := trimming(originalSubscribeTimes, start, end)

	sort.Sort(pubSort(targetSubscribeData))

	logger.Info("execute start", zap.Time("time", evaluateStartTime))

	/** 評価の計算式をここから **/
	data := eliminate(publishData, targetSubscribeData)

	evaluateData := make([]pubsub.PubSubTimeStamp, len(data.ingressData))

	originPubData := data.publishData
	ePubStamp := data.ingressData
	eSubStamp := data.egressData
	publishMessageCounter := len(originPubData)

	totalDurationNano := int64(0)
	for index := 0; index < len(ePubStamp); index++ {
		evaluateData[index].Published = ePubStamp[index]
		evaluateData[index].Subscribed = eSubStamp[index]
		totalDurationNano += eSubStamp[index].Sub(ePubStamp[index]).Nanoseconds()
	}

	fmt.Printf("tRttNanoDuration=%d\n", totalDurationNano)
	RTT := float64(totalDurationNano / int64(len(evaluateData)))
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
	originPublishThroughput := pubsub.CalMillisecondThroughput(originPubData)
	fmt.Printf("pub thoughput: %fmsg/ms\n", originPublishThroughput)

	publishThroughput := pubsub.CalMillisecondThroughput(ePubStamp)
	fmt.Printf("pub thoughput: %fmsg/ms\n", publishThroughput)

	subscribeThroughput := pubsub.CalMillisecondThroughput(eSubStamp)
	fmt.Printf("sub thoughput: %fmsg/ms\n", subscribeThroughput)

	lostNum := float64(publishMessageCounter - len(evaluateData))
	lostRatio := lostNum / float64(len(publishData)) * 100
	fmt.Printf("total msgNum:%d, lost num:%f\n", publishMessageCounter, lostNum)
	fmt.Printf("lost rate: %f%%\n", lostRatio)

	//性能限界には，評価ツールからのpublish[ms]と計測データからのsubscribe[msg/ms]を用いる
	//ratio := (sThroughput / pThroughput) * 100
	ratio := (subscribeThroughput / originPublishThroughput) * 100
	fmt.Printf("new Definition1: %f%%\n", ratio)

	/** ここまで **/
	fmt.Printf("first entry, publish:%s, subscribe:%s\n", evaluateData[0].Published, evaluateData[0].Subscribed)
	fmt.Printf("last entry, publish:%s, subscribe:%s\n", evaluateData[len(evaluateData)-1].Published, evaluateData[len(evaluateData)-1].Subscribed)

	/**
	 * データ出力していきます
	 **/
	filePath := fmt.Sprintf("/Users/hiroki/Downloads/%s.csv", exportFile)
	fp, first := newFile(filePath)
	writer := bufio.NewWriter(fp)

	if first {
		_, err := writer.WriteString(fmt.Sprintf("publish[msg/ms], ingress [msg/ms],egress [msg/ms],latecy [ms],total msg,lost msg,Definition [%%],lost rate [%%]\n"))
		if err != nil {
			logger.Error("cannot write", zap.Error(err))
		}
	}
	expotData := fmt.Sprintf("%f,%f,%f,%f,%d,%d,%f,%f\n",
		originPublishThroughput, publishThroughput, subscribeThroughput, mRTT, publishMessageCounter, int(lostNum), ratio, lostRatio)
	_, err = writer.WriteString(expotData)
	if err != nil {
		logger.Error("cannot write", zap.Error(err))
	}

	writer.Flush()
	fp.Close()

	pubsub.DisconnectALL(publishers)
	sb.Fin()
}

func newFile(fn string) (*os.File, bool) {
	_, exist := os.Stat(fn)
	fp, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		logger.Error("cannt open", zap.Error(err))
	}
	return fp, os.IsNotExist(exist)
}

func trimming(data []pubsub.PubSubTimeStamp, start time.Time, end time.Time) []pubsub.PubSubTimeStamp {
	logger.Info("check good subscribe data START", zap.Int("len(result)", len(data)))
	var result []pubsub.PubSubTimeStamp
	for _, d := range data {
		if d.Published.Sub(start) > 0 && d.Published.Sub(end) < 0 {
			result = append(result, d)
		}
	}
	logger.Info("check good subscribe data END", zap.Int("len(result)", len(result)))

	return result
}

func launcher() {
	qosFlag := flag.Int("qos", 0, "MQTT QoS(0|1|2)")
	retainFlag := flag.Bool("retain", false, "MQTT Retain")
	topicFlag := flag.String("topic", "", "Base topic")
	sizeFlag := flag.Int("size", 100, "Message size per publish (byte)")
	loadFlag := flag.Float64("load", 5, "publish/ms")
	loggingFileFlag := flag.String("logging file", "NONE", "Base file name")
	brokersFlag := flag.String("broker", "NONE.json", "json")
	flag.Parse()

	topic = *topicFlag
	size = *sizeFlag
	load = *loadFlag
	config = *loggingFileFlag
	retain = *retainFlag
	b := *brokersFlag

	configurations = pubsub.NewConfigurations()
	configurations.Qos = byte(*qosFlag)
	configurations.Topic = topic
	configurations.Size = size
	configurations.Load = load
	configurations.Retain = retain

	logger.Debug("configurations", zap.Any("config", configurations))

	exportFile = fmt.Sprintf("%s[load=%f]", b[:len(b)-5], load)
	logger.Info("export file name is", zap.String("file", exportFile))

	brokersJSON := fmt.Sprintf("./exp/%s", *brokersFlag)
	byteS, err := ioutil.ReadFile(brokersJSON)
	if err != nil {
		logger.Error("cannot read", zap.Error(err))
	}
	var brokers []broker
	if err := json.Unmarshal(byteS, &brokers); err != nil {
		logger.Error("cannot Unmarshal", zap.Error(err))
	}

	factory := pubsub.NewClientFactory()
	for _, b := range brokers {
		c := factory.GetConnected(b.Type, b.Host, b.Num)
		if b.Type == "pub" {
			publishers = append(publishers, c...)
		} else {
			subscribers = append(subscribers, c...)
		}
	}
}

func execute(publishers []mqtt.Client) []time.Time {
	//初期化
	var allPublishedTimeStamp []time.Time

	randMsg := getMessage(size - len(stampMQTT) - 1)
	maxInterval := float64(len(publishers)) / load
	timeCh := make(chan []time.Time)
	redy := sync.WaitGroup{}
	redy.Add(1)

	for index := 0; index < len(publishers); index++ {
		//debugに使っただけなので決してよい 11/15 22:50
		time.Sleep(time.Millisecond * 30)
		go func(index int) {
			var timeStamp time.Time
			var pts []time.Time
			p := publishers[index]
			topic := fmt.Sprintf("%05d", index)
			firstSleepDuration := getRandomInterval(maxInterval)

			//fmt.Printf("publish topic%s\n", topic)
			//logger.Info(fmt.Sprintf("first: %s", firstSleepDuration))
			logger.Info("Setup is complete in Publisher", zap.String("topic", topic), zap.Duration("first sleep", firstSleepDuration))

			redy.Wait()
			startTS := time.Now()
			time.Sleep(firstSleepDuration)
			for count := 0; ; count++ {
				if count > 0 {
					gap := time.Now().Sub(startTS)
					ideal := time.Duration(maxInterval * 1000 * 1000 * float64(count))
					wait := ideal - gap

					if wait > 0 {
						logger.Debug("waiting next publish",
							zap.Duration("gap", gap),
							zap.Duration("ideal", ideal),
							zap.Duration("wait", wait))
						time.Sleep(wait)
					} else {
						logger.Warn("May be publish is delayed due to lack of performance",
							zap.Duration("gap", gap),
							zap.Duration("ideal", ideal),
							zap.Duration("wait", wait))
					}
				}
				timeStamp = time.Now()
				formattedTimeStamp := timeStamp.Format(stampMQTT)
				msg := formattedTimeStamp + "/" + randMsg + topic
				token := p.Publish(topic, configurations.Qos, retain, msg)

				if token.Wait() && token.Error() != nil {
					fmt.Printf("publish error: %s\n", token.Error())
				} else {
					logger.Debug("publish is complete",
						zap.String("topic", topic), zap.String("time stamp", formattedTimeStamp))
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

func getMessage(length int) string {
	m := pubsub.NewMessage(length)
	return m.String()
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

func eliminate(filter []time.Time, data []pubsub.PubSubTimeStamp) evaData {
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
				diration := d.Published.Sub(f)
				if diration < 0 {
					in++
				} else if diration == 0 {
					ingress = append(ingress, d.Published)
					egress = append(egress, d.Subscribed)
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
