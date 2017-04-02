package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/thbkrkr/qli/client"
)

var (
	name      = "oq"
	buildDate = "dev"
	gitCommit = "dev"

	produceStream bool
	topic         string
	consumerGroup string

	initialOldestOffset     bool
	forceToConsumeFromStart bool
)

func init() {
	flag.BoolVar(&produceStream, "s", false, "Enable produce stream")
	flag.StringVar(&topic, "t", "", "Topic (default: $T)")
	flag.StringVar(&name, "n", "", "Name used to suffix the consumer group (default: oq-<hostname>)")
	flag.BoolVar(&initialOldestOffset, "o", false, "Start to the oldest offset (default: newest)")
	flag.Parse()
}

func main() {
	hostname, _ := os.Hostname()

	if topic == "" {
		topic = os.Getenv("T")
		if topic == "" {
			handlErr(errors.New("Empty topic"), "Fail to start oq")
		}
	}
	os.Setenv("T", topic)

	clientName := fmt.Sprintf("%s-%s", "oq", hostname)
	if name != "" {
		clientName = name
	}

	if initialOldestOffset {
		os.Setenv("INITIAL_OFFSET_OLDEST", "true")
	}

	q, err := client.NewClientFromEnv(clientName)
	handlErr(err, "Fail to create qli client")

	// Consume to stdout

	if nothingInStdin() {
		sub, err := q.SubOn(topic)
		handlErr(err, "Fail to create qli consumer")

		go q.CloseOnSig()

		for msg := range sub {
			fmt.Println(string(msg))
		}

		return
	}

	// or Produce stdin

	go q.CloseOnSig()

	defer q.Recover()

	stdin := bufio.NewScanner(os.Stdin)
	stdin.Scan()
	q.SendOn(topic, stdin.Bytes())

	// Async pub
	if produceStream {
		pub, err := q.AsyncPubOn(topic)
		handlErr(err, "Fail to create qli producer")

		stdin := bufio.NewScanner(os.Stdin)
		for stdin.Scan() {
			pub <- stdin.Bytes()
		}
		if err := stdin.Err(); err != nil {
			handlErr(err, "Fail to read stdin")
		}

		// Sync pub
	} else {
		pub, err := q.PubOn(topic)
		handlErr(err, "Fail to create qli producer")

		stdin := bufio.NewScanner(os.Stdin)
		for stdin.Scan() {
			pub <- stdin.Bytes()
		}
		if err := stdin.Err(); err != nil {
			handlErr(err, "Fail to read stdin")
		}
	}

}

// --

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func nothingInStdin() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		handlErr(err, "Fail to read stdin")
	}

	return stat.Mode()&os.ModeCharDevice != 0
}

func handlErr(err error, context string) {
	if err != nil {
		fmt.Printf("%s: %s\n", context, err)
		os.Exit(1)
	}
}
