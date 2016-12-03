package client

import (
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

func newSyncProducer(q *Qlient) (sarama.SyncProducer, error) {
	producer, err := sarama.NewSyncProducer([]string{q.config.Broker}, newConfig(q.config.Key, q.config.User, q.config.Password))
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func newAsyncProducer(q *Qlient) (sarama.AsyncProducer, error) {
	producer, err := sarama.NewAsyncProducer([]string{q.config.Broker}, newConfig(q.config.Key, q.config.User, q.config.Password))
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// Pub returns a channel to publish messages
func (q *Qlient) Pub() (chan string, error) {
	return q.PubByTopic(q.config.Topic)
}

// PubByTopic returns a channel to publish messages given a topic
func (q *Qlient) PubByTopic(topic string) (chan string, error) {
	if q.syncProducer == nil {
		syncProducer, err := newSyncProducer(q)
		if err != nil {
			return nil, err
		}
		q.syncProducer = syncProducer
	}

	q.pub = make(chan string)

	// Listen pub and send received messages
	go func() {
		log.Debug("Start to produce")
		for value := range q.pub {

			msg := sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(value),
			}

			partition, offset, err := q.syncProducer.SendMessage(&msg)
			if err != nil {
				log.WithError(err).Error("Fail to produce")
				q.err <- err
			}

			log.WithFields(log.Fields{
				"partition": partition,
				"offset":    offset,
				"value":     value,
				"topic":     topic,
			}).Debug("Produce successful")
		}
	}()

	return q.pub, nil
}

// Send produces one message
func (q *Qlient) Send(value string) (partition int32, offset int64, err error) {
	return q.SendByTopic(q.config.Topic, value)
}

// SendByTopic produces one message given a topic
func (q *Qlient) SendByTopic(topic string, value string) (partition int32, offset int64, err error) {
	if q.syncProducer == nil {
		syncProducer, err := newSyncProducer(q)
		if err != nil {
			return 0, 0, err
		}
		q.syncProducer = syncProducer
	}

	msg := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}

	partition, offset, err = q.syncProducer.SendMessage(&msg)
	if err != nil {
		log.WithError(err).Error("Fail to produce")
		return 0, 0, err
	}

	log.WithFields(log.Fields{
		"partition": partition,
		"offset":    offset,
		"value":     value,
		"topic":     topic,
	}).Debug("Produce successful")

	return partition, offset, err
}

// AsyncPub returns a channel to publish messages asynchronously
func (q *Qlient) AsyncPub() (chan string, error) {
	return q.AsyncPubByTopic(q.config.Topic)
}

// AsyncPubByTopic returns a channel to publish messages asynchronously given a topic
func (q *Qlient) AsyncPubByTopic(topic string) (chan string, error) {
	if q.asyncProducer == nil {
		asyncProducer, err := newAsyncProducer(q)
		if err != nil {
			return nil, err
		}
		q.asyncProducer = asyncProducer
	}

	q.pub = make(chan string)

	go func() {
		for err := range q.asyncProducer.Errors() {
			if !strings.Contains(err.Error(), "producer in process of shutting down") {
				log.WithError(err).Error("Fail to produce async message")
			}
		}
	}()

	go func() {
		for success := range q.asyncProducer.Successes() {
			log.WithFields(log.Fields{
				"offset":    success.Offset,
				"partition": success.Partition,
				"value":     success.Value,
			}).Debug("Async produce successful")
		}
	}()

	input := q.asyncProducer.Input()

	// Listen pub and send received messages
	go func() {

		for value := range q.pub {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Fail to produce: ", r)
					os.Exit(1)
					return
				}
			}()

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(value),
			}

			input <- msg

			log.WithFields(log.Fields{
				"value": value,
				"topic": topic,
			}).Debug("Produce sent")
		}
	}()

	return q.pub, nil
}

func (q *Qlient) closeProducer() {
	if q.pub != nil {
		close(q.pub)
	}

	if q.asyncProducer != nil {
		if err := q.asyncProducer.Close(); err != nil {
			log.WithError(err).Error("Fail to close asyncProducer")
			q.err <- err
		}
	}
	if q.syncProducer != nil {
		if err := q.syncProducer.Close(); err != nil {
			log.WithError(err).Error("Fail to close syncProducer")
			q.err <- err
		}
	}
}
