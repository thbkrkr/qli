package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	qli "github.com/thbkrkr/qli/bot"
)

var (
	name = flag.String("n", "sh-bot", "Bot name")
)

func main() {
	flag.Parse()

	name := os.Getenv("BOT_NAME")
	if name == "" {
		name = fmt.Printf("bot-%s-bot", time.Now())
	}

	bot := qli.NewBot(*name)
	dir := "./cmd"

	files, _ := ioutil.ReadDir(dir)
	for _, f := range files {
		name := f.Name()
		logrus.Infof("Register command %s", name)
		bot.RegisterScript(name, fmt.Sprintf("%s/%s", dir, name))
	}

	bot.Start()
}
