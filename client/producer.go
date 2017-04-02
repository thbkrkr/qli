package client

import (
	"os"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

// Send produces one message
func (q *Qlient) Send(data []byte) (partition int32, offset int64, err error) {
	return q.SendOn(q.config.Topic, data)
}

// SendOn produces one message given a topic
func (q *Qlient) SendOn(topic string, msg []byte) (partition int32, offset int64, err error) {
	if q.syncProducer == nil {
		syncProducer, err := newSyncProducer(q)
		if err != nil {
			return 0, 0, err
		}
		q.syncProducer = syncProducer
	}

	return q.syncProducer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	})
}

// Pub returns a channel to publish messages
func (q *Qlient) Pub() (chan []byte, error) {
	return q.PubOn(q.config.Topic)
}

// PubOn returns a channel to publish messages given a topic
func (q *Qlient) PubOn(topic string) (chan []byte, error) {
	if q.syncProducer != nil {
		return q.pub, nil
	}

	syncProducer, err := newSyncProducer(q)
	if err != nil {
		return nil, err
	}
	q.syncProducer = syncProducer

	q.pub = make(chan []byte)

	// Listen pub and produces the received messages to kafka

	go func() {
		log.Debug("Start to produce")
		for value := range q.pub {

			partition, offset, err := q.syncProducer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(value),
			})

			if err != nil {
				log.WithError(err).Error("Fail to produce")
				q.err <- err
			}

			log.WithFields(log.Fields{
				"partition": partition,
				"offset":    offset,
				"value":     string(value),
				"topic":     topic,
			}).Debug("Produce successful")

		}
	}()

	return q.pub, nil
}

// AsyncPub returns a channel to publish messages asynchronously
func (q *Qlient) AsyncPub() (chan []byte, error) {
	return q.AsyncPubOn(q.config.Topic)
}

// AsyncPubOn returns a channel to publish messages asynchronously given a topic
func (q *Qlient) AsyncPubOn(topic string) (chan []byte, error) {
	if q.asyncProducer != nil {
		return q.pub, nil
	}

	if q.asyncProducer == nil {
		asyncProducer, err := newAsyncProducer(q)
		if err != nil {
			return nil, err
		}
		q.asyncProducer = asyncProducer
	}

	go func() {
		for err := range q.asyncProducer.Errors() {
			if !strings.Contains(err.Error(), "producer in process of shutting down") {
				log.WithError(err).Error("Fail to produce async message")
			}
		}
	}()

	if log.GetLevel() == log.DebugLevel {
		go func() {
			for success := range q.asyncProducer.Successes() {
				log.WithFields(log.Fields{
					"offset":    success.Offset,
					"partition": success.Partition,
				}).Debug("Async produce successful")
			}
		}()
	}

	q.pub = make(chan []byte)

	// Listen pub and send received messages
	go func(in chan<- *sarama.ProducerMessage) {
		for data := range q.pub {
			defer func() {
				if r := recover(); r != nil {
					log.WithField("recover", r).Error("Recover by producing")
					os.Exit(1)
				}
			}()

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(data),
			}

			in <- msg

			log.WithFields(log.Fields{
				"topic": topic,
			}).Debug("Produce sent")
		}
	}(q.asyncProducer.Input())

	return q.pub, nil
}

func newSyncProducer(q *Qlient) (sarama.SyncProducer, error) {
	config := newSaramaConfig(q.config.User, q.config.Password)
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(q.config.Brokers, ","), config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func newAsyncProducer(q *Qlient) (sarama.AsyncProducer, error) {
	config := newSaramaConfig(q.config.User, q.config.Password)
	producer, err := sarama.NewAsyncProducer(strings.Split(q.config.Brokers, ","), config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (q *Qlient) closeProducers() {
	if q.asyncProducer != nil {
		if err := q.asyncProducer.Close(); err != nil {
			log.WithError(err).Error("Fail to close async producer")
			q.err <- err
		}
	}
	if q.syncProducer != nil {
		if err := q.syncProducer.Close(); err != nil {
			log.WithError(err).Error("Fail to close sync producer")
			q.err <- err
		}
	}

	if q.pub != nil {
		close(q.pub)
	}
}
