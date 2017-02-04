package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	qli "github.com/thbkrkr/qli/bot"
)

func main() {
	name := os.Getenv("BOT_NAME")
	if name == "" {
		hostname, _ := os.Hostname()
		now := time.Now().UnixNano()
		name = fmt.Sprintf("%s-%d~bot-bot2", hostname, now)
	}

	bot := qli.NewBot(name)

	dir := "./cmd"
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logrus.Fatal("Fail to read scripts directory " + dir)
	}

	for _, f := range files {
		name := f.Name()
		logrus.Infof("Register command %s", name)
		bot.RegisterScript(name, fmt.Sprintf("%s/%s", dir, name))
	}

	bot.Start()
}
