package client

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

// Qlient represents a qlient to produce and consume messages
type Qlient struct {
	config *Config

	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer

	consumer        *cluster.Consumer
	consumerGroupID string

	pub chan string
	sub chan string
	err chan error

	IsClosed bool
}

// NewClientFromEnv creates a new qlient using environment variables
func NewClientFromEnv(name string) (*Qlient, error) {
	conf, err := newConfigFromEnv(name)
	if err != nil {
		logrus.WithError(err).Fatal("Fail to process config")
		return nil, err
	}

	return NewClient(conf)
}

// NewClient creates a new qlient given a config
func NewClient(conf *Config) (*Qlient, error) {
	if conf.Debug {
		enableDebugLogLevel()
	}

	return &Qlient{
		config:   conf,
		err:      make(chan error),
		IsClosed: false,
	}, nil
}

// Recover handles panic trying to send on the pub closed channel when catching ctrl+c to close the qlient
func (q *Qlient) Recover() {
	if r := recover(); r != nil {
		if q.IsClosed && r == "send on closed channel" {
			os.Exit(0)
		}
	}
}

// CloseOnSig waits an interruption to close the qlient
func (q *Qlient) CloseOnSig() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)

	<-sigc
	logrus.Debug("Close client on sig")
	q.Close()
}

// Close closes the qlient
func (q *Qlient) Close() {
	q.IsClosed = true

	q.closeConsumer()
	q.closeProducer()

	if q.err != nil {
		close(q.err)
	}
}

func (q *Qlient) enableSaramaDebugLogger() {
	sarama.Logger = log.New(os.Stdout, "[sarama-debug] ", log.LstdFlags)
}
