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

var (
	broker = p("b", "", "Broker")
	key    = p("k", "", "Key")
	topic  = p("t", "", "Topic")

	name = p("n", fmt.Sprintf("qing/%d", rand.Intn(666)), "name")
)

var totalMessages = 3

func main() {
	load()

	done := make(chan bool)

	qli, err := client.NewClient(strings.Split(*broker, ","), *key, *topic, *name)
	handlErr(err, fmt.Sprintf("Fail to create qli client on %s", *broker))
	logrus.WithFields(logrus.Fields{
		"broker": *broker,
		"topic":  *topic,
	}).Infof("Start smoketest: produce and consume %d messages", totalMessages)

	now := unixTimestamp()
	s := rand.NewSource(now)
	r := rand.New(s)
	randNum := r.Int63n(10000000)

	go func() {
		consume(qli, done, randNum)
	}()

	time.Sleep(time.Duration(1) * time.Second)

	for i := 0; i < totalMessages; i++ {
		test := Test{
			Id:        i,
			Timestamp: now,
			RandNum:   randNum,
		}
		bmsg, err := json.Marshal(test)
		handlErr(err, "Fail to marshal Test struct")

		msg := string(bmsg)
		qli.Send(msg)
		logrus.WithField("msg", msg).Info("Produce")
	}

	<-done

	logrus.Info("Close")
	qli.Close()
}

type Test struct {
	Id        int
	Timestamp int64
	RandNum   int64
}

func consume(qli *client.Qlient, done chan bool, refRandNum int64) {
	count := 0
	for msg := range qli.Sub() {

		var test Test
		err := json.Unmarshal([]byte(msg), &test)
		handlErr(err, "Fail to unmarshal Test struct")

		logrus.Infof("%v", test)

		isOk := test.RandNum == refRandNum

		logrus.WithFields(logrus.Fields{
			"id":      test.Id,
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
