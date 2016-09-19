package client

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

// Qlient represents a qli client
type Qlient struct {
	brokers  []string
	clientID string
	topic    string

	producer      sarama.SyncProducer
	asyncProducer sarama.AsyncProducer

	consumer *cluster.Consumer

	client          sarama.Client
	consumer082     sarama.Consumer
	consumerGroupID string
	offsetManager   sarama.OffsetManager

	pub chan string
	sub chan string
}

// NewClient creates a new qli client
func NewClient(brokers []string, secret string, topic string, groupID string) (*Qlient, error) {
	producer, err := newProducer(brokers, secret)
	if err != nil {
		return nil, err
	}

	//log.SetLevel(log.DebugLevel)

	return &Qlient{
		brokers:  brokers,
		clientID: secret, topic: topic,
		producer:        producer,
		consumerGroupID: groupID,
	}, nil
}

// NewClientFromEnv creates a new qli client using environment variables
// to configure the brokers, the topic and the secret
func NewClientFromEnv() (*Qlient, error) {
	brokers := []string{os.Getenv("B")}
	topic := os.Getenv("T")
	secret := os.Getenv("K")
	groupID := fmt.Sprintf("qli-%s-%d", topic, time.Now().UnixNano())
	return NewClient(brokers, secret, topic, groupID)
}

// Close closes the qli client
func (c *Qlient) Close() {
	log.Debug("Close qli")
	if c.producer != nil {
		c.closeProducer()
	}
	if c.consumer != nil {
		c.closeConsumer()
	}
	if c.consumer082 != nil {
		c.closeConsumer082()
	}
}

func (c *Qlient) CloseOnSig() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	//syscall.SIGHUP,
	//syscall.SIGINT,
	//syscall.SIGTERM,
	//syscall.SIGQUIT)

	<-sigc
	c.Close()
}
