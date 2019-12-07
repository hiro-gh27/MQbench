package pubsub

import "go.uber.org/zap"

var logger *zap.Logger

func Init() {
	if logger == nil {
		logger = zap.L()
	}
}
