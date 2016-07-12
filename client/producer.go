package client

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

func newProducer(brokers []string, secret string) (sarama.SyncProducer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = secret

	producer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// Pub returns a channel to publish messages
func (q *Qlient) Pub() chan string {
	q.pub = make(chan string)

	// Listen pub and send all
	go func() {
		for value := range q.pub {
			partition, offset, err := q.producer.SendMessage(&sarama.ProducerMessage{
				Topic: q.topic,
				Value: sarama.StringEncoder(value),
			})
			if err != nil {
				log.WithError(err).Error("Fail to produce")
			}

			log.WithFields(log.Fields{
				"partition": partition,
				"offset":    offset,
				"value":     value,
				"topic":     q.topic,
			}).Debug("Produce successful")
		}
	}()

	return q.pub
}

// Send produces one message
func (q *Qlient) Send(value string) (partition int32, offset int64, err error) {
	partition, offset, err = q.producer.SendMessage(&sarama.ProducerMessage{
		Topic: q.topic,
		Value: sarama.StringEncoder(value),
	})
	if err != nil {
		log.WithError(err).Error("Fail to produce")
	}

	log.WithFields(log.Fields{
		"partition": partition,
		"offset":    offset,
		"value":     value,
		"topic":     q.topic,
	}).Debug("Produce successful")

	return partition, offset, err
}

func (q *Qlient) closeProducer() {
	if q.producer != nil {
		if err := q.producer.Close(); err != nil {
			log.WithError(err).Error("Fail to close producer")
		}
	}
	if q.pub != nil {
		close(q.pub)
	}
}
