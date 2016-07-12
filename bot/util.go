package bot

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"
)

func env(name string) string {
	value := os.Getenv(name)
	if value == "" {
		exitf("Env var %s not set", name)
	}
	return value
}

func (b *Bot) say(message string) string {
	// TODO: improve
	return fmt.Sprintf(`{"user": "%s", "message":"%s"}`, b.name,
		strings.Replace(strings.TrimSpace(message), "\n", "", -1))
}

type Message struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func unmarshal(value string) Message {
	var message Message
	if err := json.Unmarshal([]byte(value), &message); err != nil {
		log.WithError(err).WithField("string", value).Error("Fail to parse json message")
	}
	return message
}

func handleErr(err error) {
	panic(err)
}

func exitf(format string, a ...interface{}) {
	log.Errorf(format, a)
	os.Exit(1)
}

func waitSig() {
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
}
