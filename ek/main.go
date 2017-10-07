package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/thbkrkr/qli/bot"
	"github.com/thbkrkr/qli/client"
)

var (
	name      = "ek"
	buildDate = "dev"
	gitCommit = "dev"

	botID          string
	patternBotName = "ek~bot@%s-bot2"
	cmdTopic       string
	logsTopic      string
	executor       *TasksExecutor
)

func init() {
	flag.StringVar(&cmdTopic, "cmd-topic", "", "Topic to send command (override $T)")
	flag.StringVar(&logsTopic, "logs-topic", "", "Topic to send logs")
	flag.Parse()

	if cmdTopic != "" {
		os.Setenv("T", cmdTopic)
	}
	if logsTopic == "" {
		logsTopic = os.Getenv("T")
	}
}

func main() {
	b, executor := newExecutorBot(logsTopic)
	b.
		RegisterCmdFunc("ek help", func(args ...string) (string, error) {
			return help(executor.name)
		}).
		RegisterCmdFunc("ek ps", func(args ...string) (string, error) {
			return executor.ps()
		}).
		RegisterCmdFunc("ek attach", func(args ...string) (string, error) {
			return executor.attach(args)
		}).
		RegisterCmdFunc("ek gc", func(args ...string) (string, error) {
			return executor.gc()
		}).
		RegisterCmdFunc("ek kill", func(args ...string) (string, error) {
			return executor.kill(args)
		}).
		RegisterCmdFunc("ek ping", func(args ...string) (string, error) {
			return executor.exec(args)
		}).
		RegisterCmdFunc("ek uptime", func(args ...string) (string, error) {
			return executor.exec(args)
		}).
		RegisterCmdFunc("ek", func(args ...string) (string, error) {
			return executor.exec(args)
		}).
		RegisterCmdFunc("ek dps", func(args ...string) (string, error) {
			cmd := []string{"docker", "ps", "-a", "--format", `'table{{.Names}}\t{{.Status}}'`}
			return executor.exec(cmd)
		}).
		Start()
}

func newExecutorBot(logsTopic string) (*bot.Bot, *TasksExecutor) {
	hostname, _ := os.Hostname()
	b := bot.NewBot(fmt.Sprintf(patternBotName, hostname))

	q, err := client.NewClientFromEnv(b.Name)
	if err != nil {
		log.Fatal(err)
	}

	logs, err := q.PubOn(logsTopic)
	if err != nil {
		log.Fatal(err)
	}

	executor = NewTaskExecutor(b.Name, logs)

	return b, executor
}

func help(name string) (string, error) {
	return base64.StdEncoding.EncodeToString([]byte(`usage: ek COMMAND
Commands:
  cmd     Any command

  ps      List tasks
  attach  Attach task
  kill    Kill a task
  gc      Kill all tasks and remove history

  dps     docker ps
`)), nil
}
