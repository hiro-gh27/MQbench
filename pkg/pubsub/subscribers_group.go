package pubsub

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"sync"
	"time"
)

type SubscribersGroup struct {
	subscribers    []mqtt.Client
	configurations Configurations
}

type PubSubTimeStamp struct {
	Topic      string
	Published  time.Time
	Subscribed time.Time
}

func NewSubscribersGroup(subscribers []mqtt.Client) *SubscribersGroup {
	sg := &SubscribersGroup{
		subscribers: subscribers,
	}
	return sg
}

func (sb *SubscribersGroup) withConfigurations(configurations Configurations) *SubscribersGroup {
	sb.configurations = configurations
	return sb
}

func (sb *SubscribersGroup) Fin() {
	DisconnectALL(sb.subscribers)
}

func (sb *SubscribersGroup) ExecuteAsync(endLock *sync.WaitGroup) chan []PubSubTimeStamp {
	future := make(chan []PubSubTimeStamp)
	go func() {
		rStack := make([]*[]PubSubTimeStamp, len(sb.subscribers))
		for index := 0; index < len(sb.subscribers); index++ {
			id := index
			s := sb.subscribers[id]
			topic := fmt.Sprintf("%05d", id)
			var rVal []PubSubTimeStamp
			rStack[id] = &rVal

			fmt.Printf("sub topic: %s\n", topic)

			var callback mqtt.MessageHandler = func(c mqtt.Client, msg mqtt.Message) {
				var psts PubSubTimeStamp

				sst := time.Now().Format(stampMQTT)
				st, _ := time.Parse(stampMQTT, sst)
				spt := string(msg.Payload()[:35])
				pt, _ := time.Parse(stampMQTT, spt)

				// from configurations
				psts.Topic = msg.Topic()
				psts.Published = pt
				psts.Subscribed = st

				rVal = append(rVal, psts)
				//logger.Debug(fmt.Sprintf("topic:%s pub:%s, sub%s\n", psts.topic, psts.published, psts.subscribed))
			}

			token := s.Subscribe(topic, sb.configurations.Qos, callback)
			if token.Wait() && token.Error() != nil {
				fmt.Printf("Subscribe Error: %s\n", token.Error())
			}
		}

		var rvals []PubSubTimeStamp
		fmt.Println("wait start in setSubscribers")

		endLock.Wait()

		fmt.Println("wait end in setSubscribers")
		time.Sleep(5 * time.Second)
		for index := 0; index < len(sb.subscribers); index++ {
			rs := *rStack[index]
			fmt.Printf("len(rvs)=%d\n", len(rs))
			for _, val := range rs {
				rvals = append(rvals, val)
			}
		}
		future <- rvals
	}()
	return future
}
