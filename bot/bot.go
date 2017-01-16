package bot

import (
	"os"
	"os/exec"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/thbkrkr/qli/client"
)

// Bot represents a robot with a name and commands
type Bot struct {
	Name     string
	commands map[string]command

	q   *client.Qlient
	Pub chan string
	sub <-chan string
}

type command func(...string) string

// NewBot creates a new bot given a name
func NewBot(name string) *Bot {
	q, err := client.NewClientFromEnv(name)
	fatalIf(err)

	go q.CloseOnSig()

	pub, err := q.AsyncPub()
	fatalIf(err)
	sub, err := q.Sub()
	fatalIf(err)

	bot := &Bot{
		Name:     name,
		commands: map[string]command{},
		q:        q,
		Pub:      pub,
		sub:      sub,
	}

	bot.registerPingPong()
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
	log.WithField("name", b.Name).Info("Bot started")

	b.Pub <- b.Say("Yo!", false)

	for data := range b.sub {
		event, err := unmarshal(data)
		if err != nil {
			// skip
			continue
		}

		args := strings.Split(event.Message, " ")

		var commandFunc command
		var funcName string
		// try with 2 args first
		if len(args) > 1 {
			funcName = args[0] + " " + args[1]
			commandFunc = b.commands[funcName]
		}
		if commandFunc == nil {
			funcName = args[0]
			commandFunc = b.commands[funcName]
		}
		if commandFunc == nil {
			// skip
			continue
		}

		if event.User != b.Name && commandFunc != nil {
			// TODO: handle error
			result := commandFunc(args[1:]...)
			log.Debugf("result: %s", result)
			if result == "" {
				// TODO: ?
				continue
			}
			b.Pub <- b.Say(result, false)
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
		commands := []string{}
		for command := range b.commands {
			commands = append(commands, command)
		}
		return strings.Join(commands, " - ")
	}
}

func (b *Bot) registerPingPong() {
	b.commands["ping"] = func(args ...string) string {
		return "pong"
	}
}
