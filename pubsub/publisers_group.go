package pubsub

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	format = "2006-01-02T15:04:05.000000000Z07:00"
)

type PublishersGroup struct {
	configurations Configurations
}

func NewPublishersGroup(configurations Configurations) *PublishersGroup {
	ps := &PublishersGroup{
		configurations: configurations,
	}
	return ps
}

func (ps *PublishersGroup) Execute(publishers []mqtt.Client, duration time.Duration) ([]time.Time, time.Time) {
	var origin []time.Time
	size := ps.configurations.Size
	load := ps.configurations.Load
	qos := ps.configurations.Qos
	retain := ps.configurations.Retain

	randMsg := NewMessage(size - len(format) - 1).String()
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

			fmt.Printf("publish topic%s\n", topic)
			firstSleepDuration := randomInterval(maxInterval)
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
				msg := timeStamp.Format(format) + "/" + randMsg + topic
				logger.Debug(fmt.Sprintf("topic:%s, ts:%s", topic, timeStamp.Format(format)))
				//logger.Info(msg)

				token := p.Publish(topic, qos, retain, msg)
				if token.Wait() && token.Error() != nil {
					fmt.Printf("publish error: %s\n", token.Error())
				} else {
					pts = append(pts, timeStamp)
				}

				if timeStamp.Sub(startTS) > duration {
					break
				}
			}
			timeCh <- pts
		}(index)
	}

	time.Sleep(time.Second * 10)
	evaluateStartTime := time.Now()
	redy.Done()

	for index := 0; index < len(publishers); index++ {
		origin = append(origin, <-timeCh...)
	}

	st := NewSortableTime(origin)
	allPublishedTimeStamp := st.get()

	var rData []time.Time

	fmt.Printf("len(allPublishedTimeStamp:%d\n", len(allPublishedTimeStamp))
	for index := 0; index < len(allPublishedTimeStamp); index++ {
		str := allPublishedTimeStamp[index].Format(format)
		st, _ := time.Parse(str, str)
		rData = append(rData, st)
	}
	total := allPublishedTimeStamp[len(allPublishedTimeStamp)-1].Sub(allPublishedTimeStamp[0])
	millDuration := float64(total.Nanoseconds()) / math.Pow10(6)
	th := float64(len(allPublishedTimeStamp)) / millDuration
	fmt.Printf("thoughput: %fmsg/ms\n", th)

	return rData, evaluateStartTime
}

func randomInterval(max float64) time.Duration {
	var td time.Duration
	nanoMax := int(max * 1000 * 1000)
	if max > 0 {
		interval := rand.Intn(nanoMax)
		td = time.Duration(interval) * time.Nanosecond
	}
	return td
}
