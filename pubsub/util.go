package pubsub

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
	"math"
	"time"
)

func DisconnectALL(clinets []mqtt.Client) {
	for _, c := range clinets {
		c.Disconnect(500)
	}
}

func CalMillisecondThroughput(timeStamps []time.Time) float64 {
	logger.Info("cal throughput", zap.Int("timestamp", len(timeStamps)), zap.Time("first", timeStamps[0]), zap.Time("final", timeStamps[len(timeStamps)-1]))
	duration := timeStamps[len(timeStamps)-1].Sub(timeStamps[0])
	millisecondDuration := float64(duration.Nanoseconds()) / math.Pow10(6)
	throughtput := float64(len(timeStamps)) / millisecondDuration
	logger.Info("cal result", zap.Duration("total", duration), zap.Float64("msg/ms", throughtput))
	return throughtput
}
