package pubsub

import (
	"math/rand"
	"time"
)

const (
	letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits

	tsLayout  = time.StampNano + " 2006"
	stampMQTT = "2006-01-02T15:04:05.000000000Z07:00"
)

type message struct {
	inner string
}

func NewMessage(length int) *message {
	if length < 0 {
		length = 1
	}

	data := make([]byte, length)
	cache, remain := rand.Int63(), letterIdxMax
	for i := length - 1; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		idx := int(cache & letterIdxMask)
		if idx < len(letters) {
			data[i] = letters[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	m := &message{
		inner: string(data),
	}
	return m
}

func (m *message) String() string {
	return m.inner
}
