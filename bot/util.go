package bot

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
)

func env(name string) string {
	value := os.Getenv(name)
	if value == "" {
		exitf("Env var %s not set", name)
	}
	return value
}

func (b *Bot) Say(message string, b64 bool) []byte {
	if !strings.Contains(message, `"b64"`) {
		pattern := `{"user": "%s", "message":"%s", "b64":"%t"}`
		message = fmt.Sprintf(pattern, b.Name, message, b64)
	} else {
		pattern := `{"user": "%s", "message":"%s"}`
		message = fmt.Sprintf(pattern, b.Name, message)
	}
	message = strings.Replace(strings.TrimSpace(message), "\n", "", -1)
	return []byte(message)
}

type Event struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func unmarshal(value []byte) (*Event, error) {
	var event Event
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

func marshal(event Event) (string, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func handleErr(err error) {
	panic(err)
}

func exitf(format string, a ...interface{}) {
	log.Errorf(format, a)
	os.Exit(1)
}

/*func waitSig() {
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
}
*/
