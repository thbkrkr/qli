package client

import (
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

func (q *Qlient) newConsumer082() (
	sarama.Consumer, error) {

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = q.clientID

	client, err := sarama.NewClient(q.brokers, saramaConfig)
	if err != nil {
		log.WithError(err).Error("Fail to create client")
	}
	q.client = client

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.WithError(err).Error("Fail to create consumer")
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(q.consumerGroupID, q.client)
	if err != nil {
		log.WithError(err).Error("Fail to create offset manager")
	}
	q.offsetManager = offsetManager

	return consumer, nil
}

func (q *Qlient) Sub082() chan string {
	consumer082, err := q.newConsumer082()
	if err != nil {
		log.WithError(err).Error("Failed to create consumer")
		return nil
	}
	q.consumer082 = consumer082

	sub := q.Sub2(q.topic)
	q.sub = sub
	return sub
}

func (q *Qlient) NewSub(topic string) chan string {
	consumer, err := sarama.NewConsumerFromClient(q.client)
	if err != nil {
		log.WithError(err).Error("Fail to create consumer")
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(q.consumerGroupID, q.client)
	if err != nil {
		log.WithError(err).Error("Fail to create offset manager")
	}

	return q.Sub3(consumer, offsetManager, topic)
}

func (q *Qlient) Sub2(topic string) chan string {
	return q.Sub3(q.consumer082, q.offsetManager, topic)
}

// Sub returns a channel to receive messages
func (q *Qlient) Sub3(consumer sarama.Consumer, offsetManager sarama.OffsetManager, topic string) chan string {
	sub := make(chan string)

	partitions, err := q.consumer082.Partitions(topic)
	if err != nil {
		log.WithError(err).Error("Fail to get partitions")
	}

	// Consume all topic partitions
	wg := sync.WaitGroup{}
	for _, p := range partitions {
		wg.Add(1)
		go func(p int32) {
			consumePartition(sub, consumer, topic, p, offsetManager)
		}(p)
	}

	return sub
}

func consumePartition(sub chan string, consumer sarama.Consumer, topic string, partition int32, offsetManager sarama.OffsetManager) {
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
	if err != nil {
		log.WithError(err).Fatal("Fail to manage partition")
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

func (q *Qlient) closeConsumer082() {
	if q.consumer082 != nil {
		if err := q.consumer082.Close(); err != nil {
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
