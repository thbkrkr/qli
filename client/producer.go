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
	syncProducer, ok := q.syncProducers[topic]
	if !ok {
		var err error
		syncProducer, err = newSyncProducer(q)
		if err != nil {
			return 0, 0, err
		}
		q.syncProducers[topic] = syncProducer
	}

	return syncProducer.SendMessage(&sarama.ProducerMessage{
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
	pub, ok := q.pubs[topic]
	if ok {
		return pub, nil
	}

	syncProducer, ok := q.syncProducers[topic]
	if !ok {
		var err error
		syncProducer, err = newSyncProducer(q)
		if err != nil {
			return nil, err
		}
		q.syncProducers[topic] = syncProducer

		pubChan := make(chan []byte)

		// Listen pub and produces the received messages to kafka

		go func(pub chan []byte) {
			log.Debug("Start to produce")
			for value := range pub {

				partition, offset, err := syncProducer.SendMessage(&sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.ByteEncoder(value),
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
		}(pubChan)

		q.pubs[topic] = pubChan
	}

	return q.pubs[topic], nil
}

// AsyncPub returns a channel to publish messages asynchronously
func (q *Qlient) AsyncPub() (chan []byte, error) {
	return q.AsyncPubOn(q.config.Topic)
}

// AsyncPubOn returns a channel to publish messages asynchronously given a topic
func (q *Qlient) AsyncPubOn(topic string) (chan []byte, error) {
	pub, ok := q.apubs[topic]
	if ok {
		return pub, nil
	}

	asyncProducer, err := newAsyncProducer(q, topic)
	if err != nil {
		return nil, err
	}
	q.asyncProducers[topic] = asyncProducer

	return q.apubs[topic], nil
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

func newAsyncProducer(q *Qlient, topic string) (sarama.AsyncProducer, error) {
	config := newSaramaConfig(q.config.User, q.config.Password)
	asyncProducer, err := sarama.NewAsyncProducer(strings.Split(q.config.Brokers, ","), config)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range asyncProducer.Errors() {
			if !strings.Contains(err.Error(), "asyncProducer in process of shutting down") {
				log.WithError(err).Error("Fail to produce async message")
			}
		}
	}()

	if log.GetLevel() == log.DebugLevel {
		go func() {
			for success := range asyncProducer.Successes() {
				log.WithFields(log.Fields{
					"offset":    success.Offset,
					"partition": success.Partition,
				}).Debug("Async produce successful")
			}
		}()
	}

	q.apubs[topic] = make(chan []byte)

	// Listen pub and send received messages
	go func(in chan<- *sarama.ProducerMessage, p chan []byte) {
		for data := range p {
			defer func() {
				if r := recover(); r != nil {
					log.WithField("recover", r).Error("Recover by async producing")
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
			}).Debug("Async produce sent")
		}
	}(asyncProducer.Input(), q.apubs[topic])

	return asyncProducer, nil
}

func (q *Qlient) closeProducers() {
	for _, sp := range q.syncProducers {
		if sp != nil {
			if err := sp.Close(); err != nil {
				log.WithError(err).Error("Fail to close sync producer")
				q.err <- err
			}
		}
	}
	for _, asp := range q.asyncProducers {
		if asp != nil {
			if err := asp.Close(); err != nil {
				log.WithError(err).Error("Fail to close async producer")
				q.err <- err
			}
		}
	}
	for _, pub := range q.pubs {
		if pub != nil {
			close(pub)
		}
	}
	for _, apub := range q.apubs {
		if apub != nil {
			close(apub)
		}
	}
}
