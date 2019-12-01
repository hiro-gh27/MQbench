package pubsub

import "fmt"

type Configurations struct {
	//Logger      *zap.Logger
	Qos         byte
	Retain      bool
	Topic       string
	Size        int
	BasePayload string
	Load        float64
	config      string
	Number      int
}

func NewConfigurations() *Configurations {
	o := &Configurations{
		Qos:         0,
		Retain:      false,
		Topic:       "",
		Size:        100,
		BasePayload: "",
		Load:        3,
		config:      "",
		Number:      10,
	}
	return o
}

func (c *Configurations) WithQos(qos byte) *Configurations {
	c.Qos = qos
	return c
}

func (c *Configurations) WithRetain(bool bool) *Configurations {
	c.Retain = bool
	return c
}

func (c *Configurations) WithTopic(topic string) *Configurations {
	c.Topic = topic
	return c
}

func (c *Configurations) WithSize(size int) *Configurations {
	c.Size = size
	return c
}

func (c *Configurations) WithBasePayload(payload string) *Configurations {
	c.BasePayload = payload
	return c
}

func (c *Configurations) WithLoad(load float64) *Configurations {
	c.Load = load
	return c
}

func (c *Configurations) WithConfig(config string) *Configurations {
	c.config = config
	return c
}

func (c *Configurations) WithNumber(number int) *Configurations {
	c.Number = number
	return c
}

func (c *Configurations) String() string {
	return fmt.Sprintf("%+v", *c)
}
