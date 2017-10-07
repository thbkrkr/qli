package client

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

// Qlient represents a qlient to produce and consume messages
type Qlient struct {
	config *Config

	syncProducers  map[string]sarama.SyncProducer
	asyncProducers map[string]sarama.AsyncProducer
	consumers      map[string]*cluster.Consumer

	err   chan error
	pubs  map[string]chan []byte
	apubs map[string]chan []byte
	subs  map[string]chan []byte

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
		config:         conf,
		syncProducers:  map[string]sarama.SyncProducer{},
		asyncProducers: map[string]sarama.AsyncProducer{},
		consumers:      map[string]*cluster.Consumer{},
		err:            make(chan error),
		pubs:           map[string]chan []byte{},
		apubs:          map[string]chan []byte{},
		subs:           map[string]chan []byte{},
		IsClosed:       false,
	}, nil
}

// Recover handles panic trying to send on the pub closed channel when catching ctrl+c to close the qlient
func (q *Qlient) Recover() {
	if r := recover(); r != nil {
		if q.IsClosed && r == "send on closed channel" {
			logrus.Info("Recover while sending message on closed channel")
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
	time.Sleep(2 * time.Second)
}

// Close closes the qlient
func (q *Qlient) Close() {
	q.IsClosed = true

	q.closeConsumers()
	q.closeProducers()

	if q.err != nil {
		close(q.err)
	}
}

func (q *Qlient) enableSaramaDebugLogger() {
	sarama.Logger = log.New(os.Stdout, "[sarama-debug] ", log.LstdFlags)
}
