package pubsub

import "time"

type latency struct {
	topic      string
	published  time.Time
	subscribed time.Time
	value      int
}

func NewLatency(topic string, pub time.Time, sub time.Time) *latency {
	l := &latency{
		topic:      topic,
		published:  pub,
		subscribed: sub,
	}
	return l
}
