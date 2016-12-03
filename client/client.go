package client

import (
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
	"github.com/kelseyhightower/envconfig"
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

// Config represents a qlient configuration.
// The groupID is equal to '<key>.<name>' or '<user>.<name>'.
// SASL/SSL is enabled if the <password> is not empty.
type Config struct {
	Name   string
	Broker string `envconfig:"b" required:"true"`
	Topic  string `envconfig:"t" required:"true"`

	Key      string `envconfig:"k"`
	User     string `envconfig:"u"`
	Password string `envconfig:"p"`

	GroupID string `envconfig:"g"`
}

// NewConfigFromEnv creates a new qlient configuration using environment variables:
// the broker url (B), the topic (T), the clientID (K)
// and user (U) and password (P) to use SASL/SSL
func NewConfigFromEnv(name string) (*Config, error) {
	var conf Config
	err := envconfig.Process("", &conf)
	if err != nil {
		logrus.WithError(err).Fatal("Fail to process config")
		return nil, err
	}

	conf.Name = name

	if conf.User == "" {
		conf.User = strings.Split(conf.Key, "-")[0]
	} else {
		conf.Key = conf.User
	}
	if conf.GroupID == "" {
		conf.GroupID = conf.User + "." + name
	}

	logrus.Info(conf.GroupID)

	return &conf, nil
}

// NewClientFromEnv creates a new qlient using environment variables
func NewClientFromEnv(name string) (*Qlient, error) {
	conf, err := NewConfigFromEnv(name)
	if err != nil {
		logrus.WithError(err).Fatal("Fail to process config")
		return nil, err
	}

	return NewClient(conf)
}

// NewClient creates a new qlient given a config
func NewClient(conf *Config) (*Qlient, error) {
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
	q.Close()
}

// Close closes the qlient
func (q *Qlient) Close() {
	q.IsClosed = true
	logrus.Debug("set closed")

	q.closeProducer()

	if q.consumer != nil {
		q.closeConsumer()
	}
	if q.err != nil {
		close(q.err)
	}
}

func (q *Qlient) enableSaramaDebugLogger() {
	sarama.Logger = log.New(os.Stdout, "[sarama-debug] ", log.LstdFlags)
}
