package bot

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/thbkrkr/qli/client"
)

// Bot represents a robot with a name and commands
type Bot struct {
	name     string
	commands map[string]command

	q   *client.Qlient
	pub chan<- string
	sub <-chan string
}

type command func(...string) string

// NewBot creates a new bot given a name
func NewBot(name string) *Bot {
	q, err := client.NewClientFromEnv(name)
	fatalIf(err)

	hostname, _ := os.Hostname()

	pub, err := q.Pub()
	fatalIf(err)

	bot := &Bot{
		name:     fmt.Sprintf("%s-%s-%d", name, hostname, rand.Intn(100)),
		commands: map[string]command{},
		q:        q,
		pub:      pub,
		sub:      q.Sub(),
	}

	log.Infof("Bot %s started", bot.name)
	bot.registerHelp()

	return bot
}

func fatalIf(err error) {
	if err != nil {
		log.WithError(err).Error("Fail to create qli bot client")
		os.Exit(1)
	}
}

// Start start a bot
func (b *Bot) Start() {
	log.Infof("Start bot %s", b.name)

	go func() {
		waitSig()
		b.q.Close()
	}()

	b.pub <- b.say("Yo!")

	for data := range b.sub {
		message := unmarshal(data)

		args := strings.Split(message.Message, " ")
		name := args[0]

		commandFunc := b.commands[name]

		log.Infof("command: %v", name)

		if message.User != b.name && commandFunc != nil {
			// TODO: handle error
			result := commandFunc(args[1:]...)
			b.pub <- b.say(result)
		}
	}

}

// Command registrations

func (b *Bot) RegisterCmdFunc(name string, c command) *Bot {
	b.commands[name] = c
	return b
}

func (b *Bot) RegisterCmd(name string, command string, args ...string) *Bot {
	b.commands[name] = func(args ...string) string {
		stdout, err := exec.Command(name, args...).Output()
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"name": name, "args": args,
			}).Error("Fail to exec command")
		}
		return string(stdout)
	}

	return b
}

func (b *Bot) RegisterScript(name string, scriptPath string, args ...string) *Bot {
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		exitf("Script not found: %s", scriptPath)
	}

	b.commands[name] = func(args ...string) string {
		stdout, err := exec.Command(scriptPath, args...).Output()
		if err != nil {
			log.Warn("err: ", err)
		}
		return string(stdout)
	}
	return b
}

func (b *Bot) registerHelp() {
	b.commands["help"] = func(args ...string) string {
		commandList := ""
		for command, _ := range b.commands {
			commandList += " - " + command
		}
		return commandList
	}
}
