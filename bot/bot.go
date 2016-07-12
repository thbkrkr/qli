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

	pub chan<- string
	sub <-chan string
}

type command func(...string) string

func NewBot(name string) *Bot {
	broker := env("B")
	secret := env("K")
	topic := env("T")

	qli, err := client.NewClient([]string{broker}, secret, topic, "bot-"+name)
	if err != nil {
		log.Error("Fail to create qli bot client")
		os.Exit(1)
	}

	hostname, _ := os.Hostname()

	bot := &Bot{
		name:     fmt.Sprintf("%s-%s-%d", name, hostname, rand.Intn(100)),
		commands: map[string]command{},
		pub:      qli.Pub(),
		sub:      qli.Sub(),
	}

	log.Infof("Bot %s started", bot.name)
	bot.registerHelp()

	return bot
}

func (b *Bot) Start() {
	log.Infof("Start bot %s", b.name)

	b.pub <- b.say("Yo!")

	for data := range b.sub {
		message := unmarshal(data)

		m := strings.Split(message.Message, " ")
		name := m[0]
		args := m[1:]

		commandFunc := b.commands[name]

		log.Warnf("command: %v", name)
		log.Warnf("commandFunc: %v", commandFunc)
		log.Warnf("b.commands: %v", b.commands)

		if message.User != b.name && commandFunc != nil {
			// TODO: handle error
			result := commandFunc(args...)
			b.pub <- b.say(message.Message + " > " + result)
		}
	}

	waitSig()
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
			log.Error("err: ", err)
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
			log.Fatal("err: ", err)
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
