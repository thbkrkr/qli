package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/thbkrkr/qli/client"
)

var totalMessages = 3

func main() {
	load()

	done := make(chan bool)

	c, err := client.NewConfigFromEnv(fmt.Sprintf("qing-%d", rand.Intn(666)))
	handlErr(err, fmt.Sprintf("Fail to create qli client"))

	q, err := client.NewClient(c)
	handlErr(err, fmt.Sprintf("Fail to create qli client"))

	logrus.WithFields(logrus.Fields{
		"broker": c.Broker,
		"topic":  c.Topic,
	}).Infof("Start smoketest: produce and consume %d messages", totalMessages)

	randNum := rand.New(rand.NewSource(unixTimestamp())).Int63n(10000000)

	go func() {
		consume(q, done, randNum)
	}()

	time.Sleep(time.Duration(1) * time.Second)

	for i := 0; i < totalMessages; i++ {
		produce(q, randNum)
	}

	<-done

	logrus.Info("Close")
	q.Close()
}

type Test struct {
	Timestamp int64
	RandNum   int64
}

func produce(qli *client.Qlient, randNum int64) {
	test := Test{
		Timestamp: unixTimestamp(),
		RandNum:   randNum,
	}
	bmsg, err := json.Marshal(test)
	handlErr(err, "Fail to marshal Test struct")

	msg := string(bmsg)
	qli.Send(msg)
	logrus.WithField("msg", msg).Info("Produce")
}

func consume(qli *client.Qlient, done chan bool, randNum int64) {
	count := 0
	for msg := range qli.Sub() {

		var test Test
		err := json.Unmarshal([]byte(msg), &test)
		handlErr(err, "Fail to unmarshal Test struct")

		logrus.Infof("%v", test)

		isOk := test.RandNum == randNum

		logrus.WithFields(logrus.Fields{
			"isOk":    isOk,
			"randNum": test.RandNum,
			"diff":    unixTimestamp() - test.Timestamp,
		}).Info("Consume")

		count++
		if count == totalMessages {
			done <- true
			break
		}
	}
}

func unixTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
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
		value := os.Getenv(strings.ToUpper(param.shortname))
		if value != "" {
			*param.val = value
		}
		if *param.val == "" {
			logrus.Fatalf("Param '%s' required (flag %s)", param.name, param.shortname)
		}
	}

}

func handlErr(err error, context string) {
	if err != nil {
		logrus.WithError(err).Error(context)
		os.Exit(1)
	}
}
