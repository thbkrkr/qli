package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/thbkrkr/qli/client"
)

var (
	name          = "smoketest"
	totalMessages = 3
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	done := make(chan bool)

	os.Setenv("INITIAL_OFFSET_OLDEST", "true")

	q, err := client.NewClientFromEnv(fmt.Sprintf("%s-%d", name, rand.Intn(666)))
	handlErr(err, fmt.Sprintf("Fail to create qli client"))

	logrus.WithFields(logrus.Fields{
		"broker": os.Getenv("B"),
		"topic":  os.Getenv("T"),
	}).Infof("Start smoketest: produce and consume %d messages", totalMessages)

	randNum := rand.New(rand.NewSource(unixTimestamp())).Int63n(10000000)

	// Start to consume
	go func() {
		consume(q, done, randNum)
	}()

	// Start to produce
	for i := 0; i < totalMessages; i++ {
		produce(q, randNum)
	}

	<-done
	q.Close()
}

type Test struct {
	Timestamp int64
	RandNum   int64
}

func produce(qli *client.Qlient, randNum int64) {
	msg, err := json.Marshal(Test{
		Timestamp: unixTimestamp(),
		RandNum:   randNum,
	})
	handlErr(err, "Fail to marshal Test struct")

	_, _, err = qli.Send(msg)
	handlErr(err, "Fail to send test message")
	logrus.WithField("msg", string(msg)).Debug("Produce")
}

func consume(qli *client.Qlient, done chan bool, randNum int64) {
	sub, err := qli.Sub()
	handlErr(err, "Fail to create qli consumer")

	count := 0
	for msg := range sub {

		var test Test
		err := json.Unmarshal(msg, &test)
		handlErr(err, "Fail to unmarshal Test struct")

		isOk := test.RandNum == randNum

		logrus.WithFields(logrus.Fields{
			"isOk":    isOk,
			"randNum": test.RandNum,
			"diff":    unixTimestamp() - test.Timestamp,
		}).Debug("Consume")

		count++
		if count == totalMessages {
			logrus.WithField("count", count).Info("OK")
			done <- true
			break
		}
	}
}

func unixTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func handlErr(err error, context string) {
	if err != nil {
		logrus.WithError(err).Error(context)
		os.Exit(1)
	}
}
