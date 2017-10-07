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
	Name    string
	Brokers string `envconfig:"b" required:"true"`
	Topic   string `envconfig:"t" required:"true"`

	User     string `envconfig:"u"`
	Password string `envconfig:"p"`
	Key      string `envconfig:"k"`

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

	// If no password, use the key as client.id
	if conf.Password == "" && conf.Key != "" {
		conf.User = conf.Key
	}

	// If no consumer group id, use the user suffixed by the name
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
	clusterConfig.Config = *newSaramaConfig(user, password)

	clusterConfig.Group.Return.Notifications = true
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	if os.Getenv("INITIAL_OFFSET_OLDEST") == "true" {
		clusterConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	return clusterConfig
}

func enableDebugLogLevel() {
	sarama.Logger = log.New(os.Stdout, "[sarama-debug] ", log.LstdFlags)
}
