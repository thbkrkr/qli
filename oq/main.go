package main

import (
	"bufio"
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
)

func init() {
	flag.BoolVar(&produceStream, "s", false, "Enable produce stream")
	flag.StringVar(&topic, "t", "", "Topic (override $T)")
	flag.Parse()
}

func main() {
	hostname, _ := os.Hostname()

	if topic != "" {
		os.Setenv("T", topic)
	}

	q, err := client.NewClientFromEnv(fmt.Sprintf("%s-%s", "oq", hostname))
	handlErr(err, "Fail to create qli client")

	// Consume to stdout

	if nothingInStdin() {
		go q.CloseOnSig()

		for msg := range q.Sub() {
			fmt.Println(msg)
		}

		return
	}

	// or Produce stdin

	go q.CloseOnSig()

	defer q.Recover()

	stdin := bufio.NewScanner(os.Stdin)
	stdin.Scan()
	q.Send(stdin.Text())

	if produceStream {
		pub, err := q.AsyncPub()
		handlErr(err, "Fail to create qli produce")

		stdin := bufio.NewScanner(os.Stdin)
		for stdin.Scan() {
			pub <- stdin.Text()
		}
		if err := stdin.Err(); err != nil {
			handlErr(err, "Fail to read stdin")
		}

	} else {
		pub, err := q.Pub()
		handlErr(err, "Fail to create qli produce")

		stdin := bufio.NewScanner(os.Stdin)
		for stdin.Scan() {
			pub <- stdin.Text()
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
