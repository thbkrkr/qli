package client

import (
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

// Sub returns a channel to receive Kafka messages
func (q *Qlient) Sub() (chan []byte, error) {
	return q.SubOn(q.config.Topic)
}

// SubOn returns a channel to receive Kafka messages given a topic
func (q *Qlient) SubOn(topic string) (chan []byte, error) {
	sub, ok := q.subs[topic]
	if !ok {
		var err error
		sub, err = q.newConsumer(topic)
		if err != nil {
			return nil, err
		}
	}
	return sub, nil
}

func (q *Qlient) newConsumer(topic string) (chan []byte, error) {
	consumer, err := cluster.NewConsumer(strings.Split(q.config.Brokers, ","),
		q.config.GroupID, []string{topic}, newSaramaClusterConfig(q.config.User, q.config.Password))
	if err != nil {
		return nil, err
	}

	q.consumers[topic] = consumer
	q.subs[topic] = make(chan []byte)

	go func(c *cluster.Consumer) {
		for err := range c.Errors() {
			log.WithField("topic", topic).WithError(err).Error("Consumer error")
			q.err <- err
		}
	}(consumer)

	go func(c *cluster.Consumer) {
		for note := range c.Notifications() {
			log.WithField("note", note).Debug("Consumer notification")
		}
	}(consumer)

	go func(c *cluster.Consumer, s chan []byte) {
		for msg := range c.Messages() {
			c.MarkOffset(msg, q.config.GroupID)

			// Send kafka messages in the sub channel
			s <- msg.Value

			log.WithFields(log.Fields{
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"topic":     msg.Topic,
				"value":     string(msg.Value),
			}).Debug("Consume successful")
		}
	}(consumer, q.subs[topic])

	log.Debug("Start to consume topic " + topic)
	return q.subs[topic], nil
}

func (q *Qlient) CloseConsumer(topic string) {
	c, ok := q.consumers[topic]
	if ok {
		if err := c.Close(); err != nil {
			log.WithError(err).Error("Fail to close consumer")
			q.err <- err
		}
	}
	sub, ok := q.subs[topic]
	if ok {
		close(sub)
	}
}

func (q *Qlient) closeConsumers() {
	for _, c := range q.consumers {
		if c != nil {
			if err := c.Close(); err != nil {
				log.WithError(err).Error("Fail to close consumer")
				q.err <- err
			}
		}
	}
	for _, sub := range q.subs {
		if sub != nil {
			close(sub)
		}
	}
}
