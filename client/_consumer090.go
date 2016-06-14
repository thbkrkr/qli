package client

/*
import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

func newBsmSaramaClusterConsumer(secret string, topic string) (*cluster.Consumer, error) {
	clusterConfig := cluster.NewConfig()
	clusterConfig.ClientID = secret
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	clusterConfig.Consumer.Fetch.Max = 1

	consumerGroupID := "qaas-client" + "-" + topic + "-" + fmt.Sprintf("%d", rand.Intn(10000))
	consumer, err := cluster.NewConsumer(brokers, consumerGroupID, []string{topic}, clusterConfig)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (q *QaasClient) ConsumeBsm() string {
	defer func() {
		if err := q.consumer.Close(); err != nil {
			log.WithError(err).Error("Failed to close consumer")
		}
	}()

	go func() {
		for err := range q.consumer.Errors() {
			log.WithError(err).Error("Failed to consume")
		}
	}()

	go func() {
		for note := range q.consumer.Notifications() {
			log.WithField("note", note).Info("Consumer notification")
		}
	}()

	log.Infoln("Start to consume topic " + q.topic)

	msg := <-q.consumer.Messages()
	q.consumer.MarkOffset(msg, "")

	//for msg := range q.consumer.Messages() {
	value := string(msg.Value)
	log.WithFields(log.Fields{
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"value":     value,
	}).Info("Consume successful")

	//fmt.Println(value)
	//}

	return value
}
*/
