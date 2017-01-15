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

var totalMessages = 3

func main() {
	done := make(chan bool)

	q, err := client.NewClientFromEnv(fmt.Sprintf("qing-%d", rand.Intn(666)))
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

	time.Sleep(time.Duration(200) * time.Millisecond)

	// Start to produce
	for i := 0; i < totalMessages; i++ {
		produce(q, randNum)
	}

	<-done

	logrus.Debug("Close")
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
	logrus.WithField("msg", msg).Debug("Produce")
}

func consume(qli *client.Qlient, done chan bool, randNum int64) {
	count := 0
	for msg := range qli.Sub() {

		var test Test
		err := json.Unmarshal([]byte(msg), &test)
		handlErr(err, "Fail to unmarshal Test struct")

		logrus.Debugf("%v", test)

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

// --

func handlErr(err error, context string) {
	if err != nil {
		logrus.WithError(err).Error(context)
		os.Exit(1)
	}
}
