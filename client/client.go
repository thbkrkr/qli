package client

import (
	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
)

// Qlient represents a qli client
type Qlient struct {
	secret string
	topic  string

	producer      sarama.SyncProducer
	consumer      sarama.Consumer
	offsetManager sarama.OffsetManager

	pub chan string
	sub chan string
}

// NewClient creates a new qli client
func NewClient(brokers []string, secret string, topic string, groupID string) (*Qlient, error) {
	producer, err := newProducer(brokers, secret)
	if err != nil {
		return nil, err
	}

	consumer, offsetManager, err := newConsumer(brokers, secret, topic, groupID)
	if err != nil {
		return nil, err
	}

	logrus.Debug("Qli client started")

	return &Qlient{
		secret: secret, topic: topic,
		producer: producer, consumer: consumer, offsetManager: offsetManager,
	}, nil
}

// Close closes the qli client
func (c *Qlient) Close() {
	c.closeProducer()
	c.closeConsumer()
}
