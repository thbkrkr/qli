package client

import (
	"fmt"
	"os"
	"time"

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

// NewClientFromEnv creates a new qli client using environment variables
// to configure the brokers, the topic and the secret
func NewClientFromEnv() (*Qlient, error) {
	brokers := []string{os.Getenv("B")}
	topic := os.Getenv("T")
	secret := os.Getenv("K")
	groupID := fmt.Sprintf("group-%s-%d", topic, time.Now().UnixNano())
	return NewClient(brokers, secret, topic, groupID)
}

// Close closes the qli client
func (c *Qlient) Close() {
	c.closeProducer()
	c.closeConsumer()
}
