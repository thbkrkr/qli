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

type command func(...string) (string, error)

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

		var cmd command
		var funcName string
		// try with 2 args first
		if len(args) > 1 {
			funcName = args[0] + " " + args[1]
			cmd = b.commands[funcName]
		}
		if cmd == nil {
			funcName = args[0]
			cmd = b.commands[funcName]
		}
		if cmd == nil {
			// skip
			continue
		}

		if event.User != b.Name && cmd != nil {
			go func(p []string) {
				resp, err := cmd(p...)
				if resp == "" {
					resp = "Ok. Empty response."
				}
				if err != nil {
					resp = err.Error()
				}
				b.Pub <- b.Say(resp, false)
			}(args[1:])
		}
	}

}

// Command registrations

func (b *Bot) RegisterCmdFunc(name string, c command) *Bot {
	b.commands[name] = c
	return b
}

func (b *Bot) RegisterCmd(name string, command string, args ...string) *Bot {
	b.commands[name] = func(args ...string) (string, error) {
		stdout, err := exec.Command(name, args...).Output()
		return string(stdout), err
	}

	return b
}

func (b *Bot) RegisterScript(name string, scriptPath string, args ...string) *Bot {
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		exitf("Script not found: %s", scriptPath)
	}

	b.commands[name] = func(args ...string) (string, error) {
		stdout, err := exec.Command(scriptPath, args...).Output()
		return string(stdout), err
	}
	return b
}

func (b *Bot) registerHelp() {
	b.commands["help"] = func(args ...string) (string, error) {
		commands := []string{}
		for command := range b.commands {
			commands = append(commands, command)
		}
		return "commands: " + strings.Join(commands, " - "), nil
	}
}

func (b *Bot) registerPingPong() {
	b.commands["ping"] = func(args ...string) (string, error) {
		return "pong", nil
	}
}
