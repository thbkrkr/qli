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
	Pub chan []byte
	sub <-chan []byte
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
		log.WithField("data", string(data)).Debug("get event")
		event, err := unmarshal(data)
		if err != nil {
			// skip
			continue
		}
		log.WithField("event", event).Debug("get event")

		// skip self message
		if event.User == b.Name {
			continue
		}

		var cmd command
		message := strings.Split(event.Message, " ")

		// try with 2 args first
		if len(message) > 1 {
			cmd = b.commands[message[0]+" "+message[1]]
		}
		// try with 1 arg
		if cmd == nil {
			cmd = b.commands[message[0]]
		}
		// skip if no command found
		if cmd == nil {
			log.WithField("message", message).Debug("cmd nil")
			// skip
			continue
		}

		// execute the command
		go func(args []string) {
			resp, err := cmd(args...)
			if resp == "" {
				// nothing to return
				return
			}
			if err != nil {
				resp = err.Error()
			}
			b.Pub <- b.Say(resp, false)
		}(message[1:])
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
