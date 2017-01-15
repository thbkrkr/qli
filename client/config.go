package client

import (
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
	"github.com/kelseyhightower/envconfig"
)

// Config represents a qlient configuration.
// The groupID is equal to '<key>.<name>' or '<user>.<name>'.
// SASL/SSL is enabled if the <password> is not empty.
type Config struct {
	Name   string
	Broker string `envconfig:"b" required:"true"`
	Topic  string `envconfig:"t" required:"true"`

	Key      string `envconfig:"k"`
	User     string `envconfig:"u"`
	Password string `envconfig:"p"`

	GroupID string `envconfig:"g"`

	Debug bool `envconfig:"d"`
}

// NewConfigFromEnv creates a new qlient configuration using environment variables:
// the broker url (B), the topic (T), the clientID (K)
// and user (U) and password (P) to use SASL/SSL
func newConfigFromEnv(name string) (*Config, error) {
	var conf Config
	err := envconfig.Process("", &conf)
	if err != nil {
		logrus.WithError(err).Fatal("Fail to process config")
		return nil, err
	}

	conf.Name = name

	// If no password, use K as client.id by setting the use
	// sasl
	if conf.Password == "" {
		conf.User = conf.Key
	}

	if conf.GroupID == "" {
		conf.GroupID = conf.User + "." + name
	}

	return &conf, nil
}

func newSaramaConfig(user string, password string) *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = user

	if password == "" {
		return config
	}

	config.Net.TLS.Enable = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = user
	config.Net.SASL.Password = password

	return config
}

func newSaramaClusterConfig(user string, password string) *cluster.Config {
	clusterConfig := cluster.NewConfig()
	clusterConfig.ClientID = user
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true

	if password == "" {
		return clusterConfig
	}

	clusterConfig.ClientID = user
	clusterConfig.Net.TLS.Enable = true
	clusterConfig.Net.SASL.Enable = true
	clusterConfig.Net.SASL.User = user
	clusterConfig.Net.SASL.Password = password

	return clusterConfig
}

func enableDebugLogLevel() {
	sarama.Logger = log.New(os.Stdout, "[sarama-debug] ", log.LstdFlags)
}
