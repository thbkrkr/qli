package client

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

var consumer *cluster.Consumer

func (q *Qlient) newConsumer() (*cluster.Consumer, error) {
	clusterConfig := cluster.NewConfig()
	clusterConfig.ClientID = q.clientID
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true
	clusterConfig.Version = sarama.V0_10_0_0

	var err error
	consumer, err = cluster.NewConsumer(q.brokers, q.consumerGroupID, []string{q.topic}, clusterConfig)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (q *Qlient) Sub() chan string {
	consumer, err := q.newConsumer()
	if err != nil {
		log.WithError(err).Error("Failed to create consumer")
		return nil
	}
	q.consumer = consumer

	go func() {
		for err := range q.consumer.Errors() {
			log.WithError(err).Error("Failed to consume")
		}
	}()
	go func() {
		for note := range q.consumer.Notifications() {
			log.WithField("note", note).Debug("Consumer notification")
		}
	}()

	log.Debug("Start to consume topic " + q.topic)

	sub := make(chan string)
	go func() {
		for msg := range q.consumer.Messages() {
			value := string(msg.Value)
			q.consumer.MarkOffset(msg, "qli")

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
		}
	}
	if q.sub != nil {
		close(q.sub)
	}
}
