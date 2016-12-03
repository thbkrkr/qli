package client

import (
	log "github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

var consumer *cluster.Consumer

func (q *Qlient) newConsumer() (*cluster.Consumer, error) {
	return q.newConsumerByTopic(q.config.Topic)
}

func (q *Qlient) newConsumerByTopic(topic string) (*cluster.Consumer, error) {
	config := newClusterConfig(q.config.Key, q.config.User, q.config.Password)
	consumer, err := cluster.NewConsumer([]string{q.config.Broker}, q.config.GroupID, []string{topic}, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// Sub returns a channel to receive Kafka messages
func (q *Qlient) Sub() chan string {
	return q.SubByTopic(q.config.Topic)
}

// SubByTopic returns a channel to receive Kafka messages given a topic
func (q *Qlient) SubByTopic(topic string) chan string {
	consumer, err := q.newConsumer()
	if err != nil {
		log.WithError(err).Error("Failed to create consumer")
		q.err <- err
		return nil
	}
	q.consumer = consumer

	go func() {
		for err := range q.consumer.Errors() {
			log.WithError(err).Error("Failed to consume")
			q.err <- err
		}
	}()
	go func() {
		for note := range q.consumer.Notifications() {
			log.WithField("note", note).Debug("Consumer notification")
		}
	}()

	log.Debug("Start to consume topic " + topic)

	sub := make(chan string)
	go func() {
		for msg := range q.consumer.Messages() {
			value := string(msg.Value)
			q.consumer.MarkOffset(msg, q.consumerGroupID)

			// Write message consumed in the sub channel
			sub <- value

			log.WithFields(log.Fields{
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"value":     value,
				"topic":     msg.Topic,
			}).Debug("Consume successful")
		}
	}()

	q.sub = sub
	return sub
}

func (q *Qlient) closeConsumer() {
	if q.consumer != nil {
		if err := q.consumer.Close(); err != nil {
			log.WithError(err).Error("Fail to close consumer")
			q.err <- err
		}
	}
	if q.sub != nil {
		close(q.sub)
	}
}
