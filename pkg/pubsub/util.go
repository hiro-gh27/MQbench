package pubsub

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"math"
	"time"
)

func DisconnectALL(clinets []mqtt.Client) {
	for _, c := range clinets {
		c.Disconnect(500)
	}
}

func CalMillisecondThroughput(timeStamps []time.Time) float64 {
	log.Info("len:", len(timeStamps), " first:", timeStamps[0], " end", timeStamps[len(timeStamps)-1])
	duration := timeStamps[len(timeStamps)-1].Sub(timeStamps[0])
	millisecondDuration := float64(duration.Nanoseconds()) / math.Pow10(6)
	throughtput := float64(len(timeStamps)) / millisecondDuration
	return throughtput
}
