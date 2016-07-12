package client

import (
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

func newConsumer(brokers []string, secret string, topic string, groupID string) (sarama.Consumer, sarama.OffsetManager, error) {
	consumerGroupID := "group" + "-" + topic + "-" + groupID

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = secret

	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		log.WithError(err).Error("Fail to create client")
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.WithError(err).Error("Fail to create consumer")
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroupID, client)
	if err != nil {
		log.WithError(err).Error("Fail to create offset manager")
	}

	return consumer, offsetManager, nil
}

// Receive receives one message
/*func (q *Qlient) Receive() string {
	return "TODO"
}*/

// Sub returns a channel to receive messages
func (q *Qlient) Sub() chan string {
	q.sub = make(chan string)

	partitions, err := q.consumer.Partitions(q.topic)
	if err != nil {
		log.WithError(err).Error("Fail to get partitions")
	}

	// Consume all topic partitions
	wg := sync.WaitGroup{}
	for _, p := range partitions {
		wg.Add(1)
		go func(p int32) {
			consumePartition(q.sub, q.consumer, q.topic, p, q.offsetManager)
		}(p)
	}

	return q.sub
}

func consumePartition(sub chan string, consumer sarama.Consumer, topic string, partition int32, offsetManager sarama.OffsetManager) {
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
	if err != nil {
		log.WithError(err).Error("Fail to manage partition")
	}
	defer partitionOffsetManager.AsyncClose()

	offset, metadata := partitionOffsetManager.NextOffset()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Fatal(err)
	}
	defer partitionConsumer.AsyncClose()

	log.WithFields(log.Fields{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
	}).Debug("Consume partition")

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			value := string(msg.Value)

			// Write message consumed in the sub channel
			sub <- value

			log.WithFields(log.Fields{
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"value":     value,
				"topic":     msg.Topic,
			}).Debug("Consume successful")

			partitionOffsetManager.MarkOffset(msg.Offset, metadata)

		case err := <-partitionConsumer.Errors():
			if err != nil {
				log.WithError(err).Error("Consume error")
			}

		case offsetErr := <-partitionOffsetManager.Errors():
			if offsetErr != nil {
				log.WithError(offsetErr).Error("Offset error")
			}
		}
	}
}

func (q *Qlient) closeConsumer() {
	if q.consumer != nil {
		if err := q.consumer.Close(); err != nil {
			log.WithError(err).Error("Fail to close consumer")
		}
	}
	if q.offsetManager != nil {
		if err := q.offsetManager.Close(); err != nil {
			log.WithError(err).Error("Fail to close offset manager")
		}
	}
	if q.sub != nil {
		close(q.sub)
	}
}
