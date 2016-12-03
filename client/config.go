package client

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

func newConfig(clientID string, user string, password string) *sarama.Config {
	if password == "" {
		return newProxyConfig(clientID, user, password)
	}
	return newSASLSSLConfig(clientID, user, password)
}

func newClusterConfig(clientID string, user string, password string) *cluster.Config {
	if password == "" {
		return newProxyClusterConfig(clientID, user, password)
	}
	return newSASLSSLClusterConfig(clientID, user, password)
}

func newProxyConfig(clientID string, user string, password string) *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = clientID

	return config
}

func newSASLSSLConfig(clientID string, user string, password string) *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = clientID
	config.Net.TLS.Enable = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = user
	config.Net.SASL.Password = password
	return config
}

func newProxyClusterConfig(clientID string, user string, password string) *cluster.Config {
	clusterConfig := cluster.NewConfig()
	clusterConfig.ClientID = clientID
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true

	return clusterConfig
}

func newSASLSSLClusterConfig(clientID string, user string, password string) *cluster.Config {
	clusterConfig := cluster.NewConfig()
	clusterConfig.ClientID = clientID
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true
	clusterConfig.Net.TLS.Enable = true
	clusterConfig.Net.SASL.Enable = true
	clusterConfig.Net.SASL.User = user
	clusterConfig.Net.SASL.Password = password

	return clusterConfig
}
