package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"

	"github.com/thbkrkr/qli/client"
)

var (
	broker = p("b", "", "Broker")
	key    = p("k", "", "Key")
	topic  = p("t", "", "Topic")

	name = p("n", fmt.Sprintf("oq/%d", rand.Intn(666)), "name")
)

func main() {
	load()

	stat, err := os.Stdin.Stat()
	if err != nil {
		handlErr(err, "Fail to read stdin")
	}

	qli, err := client.NewClient(strings.Split(*broker, ","), *key, *topic, strings.Split(*key, "-")[0])
	handlErr(err, "Fail to create qli client")

	// Receive mode
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		for msg := range qli.Sub() {
			if msg == "-1" {
				continue
			}
			fmt.Println(msg)
		}
		return
	}

	// Produce mode
	in, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		handlErr(err, "Fail to read stdin")
	}
	if len(in) == 0 {
		handlErr(errors.New("Stdin length equals 0"), "Nothing to send")
	}
	qli.Send(string(in))
}

// --

var params = map[string]param{}

type param struct {
	shortname string
	name      string

	val *string
}

func p(shortname string, val string, name string) *string {
	p := flag.String(shortname, val, name)
	params[name] = param{shortname: shortname, name: name, val: p}
	return p
}

func load() {
	flag.Parse()

	for _, param := range params {
		envVarName := strings.ToUpper(param.shortname)
		value := os.Getenv(envVarName)
		if value != "" {
			*param.val = value
		}
		if *param.val == "" {
			fmt.Printf("Param '%s' required (flag %s or env var %s)\n", param.name, param.shortname, envVarName)
			os.Exit(1)
		}
	}

}

func handlErr(err error, context string) {
	if err != nil {
		fmt.Printf("%s: %s\n", context, err)
		os.Exit(1)
	}
}
