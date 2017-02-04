package client

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

var consumer *cluster.Consumer

// Sub returns a channel to receive Kafka messages
func (q *Qlient) Sub() (chan string, error) {
	return q.SubByTopic(q.config.Topic)
}

// SubMsg returns a channel to receive Kafka messages
func (q *Qlient) SubMsg() (chan *sarama.ConsumerMessage, error) {
	return q.SubMsgByTopic(q.config.Topic)
}

// SubByTopic returns a channel to receive Kafka messages given a topic
func (q *Qlient) SubByTopic(topic string) (chan string, error) {
	if q.consumer != nil {
		return q.sub, nil
	}

	if q.consumer == nil {
		consumer, err := q.newConsumerByTopic(topic)
		if err != nil {
			return nil, err
		}
		q.consumer = consumer
	}

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

	q.sub = make(chan string)

	go func() {
		for msg := range q.consumer.Messages() {
			value := string(msg.Value)
			q.consumer.MarkOffset(msg, q.consumerGroupID)

			// Write message consumed in the sub channel
			q.sub <- value

			log.WithFields(log.Fields{
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"value":     value,
				"topic":     msg.Topic,
			}).Debug("Consume successful")
		}
	}()

	return q.sub, nil
}

// SubMsgByTopic returns a channel to receive Kafka messages given a topic
func (q *Qlient) SubMsgByTopic(topic string) (chan *sarama.ConsumerMessage, error) {
	if q.consumer != nil {
		return q.subMsg, nil
	}

	if q.consumer == nil {
		consumer, err := q.newConsumerByTopic(topic)
		if err != nil {
			return nil, err
		}
		q.consumer = consumer
	}

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

	q.subMsg = make(chan *sarama.ConsumerMessage)

	go func() {
		for msg := range q.consumer.Messages() {
			q.consumer.MarkOffset(msg, q.consumerGroupID)

			// Write message consumed in the sub channel
			q.subMsg <- msg

			log.WithFields(log.Fields{
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"topic":     msg.Topic,
			}).Debug("Message consumed successfully")
		}
	}()

	return q.subMsg, nil
}

func (q *Qlient) newConsumerByTopic(topic string) (*cluster.Consumer, error) {
	config := newSaramaClusterConfig(q.config.User, q.config.Password)
	consumer, err := cluster.NewConsumer([]string{q.config.Broker}, q.config.GroupID, []string{topic}, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
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
	if q.subMsg != nil {
		close(q.subMsg)
	}
}
