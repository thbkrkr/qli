package controllers

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/thbkrkr/qli/client"
	"golang.org/x/net/websocket"
)

type WsCtrl struct {
	Brokers string
	Key     string
	Topic   string
	Verbose bool
}

type message struct {
	Message string `json:"message"`
}

type msg struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func (t *WsCtrl) RawStream(c *gin.Context) {
	consumerGroupID := "qli-ws" + fmt.Sprintf("%d", rand.Intn(100))
	qli, err := client.NewClient(strings.Split(t.Brokers, ","), t.Key, t.Topic, consumerGroupID)
	if err != nil {
		// FIXME
		return
	}

	roomOut := qli.Sub()

	c.Status(200)

	go func() {
		for {
			m := <-roomOut
			_, err := c.Writer.Write([]byte(m + "\n"))
			if err != nil {
				log.WithError(err).Error("Sending message from qaas to ws")
				break
			}
			c.Writer.Flush()
		}
	}()

	select {}
}

func (c *WsCtrl) Consume(ws *websocket.Conn) {
	consumerGroupID := "qli-ws" + fmt.Sprintf("%d", rand.Intn(100))
	qli, err := client.NewClient(strings.Split(c.Brokers, ","), c.Key, c.Topic, consumerGroupID)
	if err != nil {
		// FIXME
		return
	}

	log.WithField("consumerGroupID", consumerGroupID).Info("New WS connection")

	// Send each message received from the websocket

	roomIn := qli.Pub()

	go func() {
		for {
			var m message

			if err := websocket.JSON.Receive(ws, &m); err != nil {
				log.WithError(err).Error("Sending message to qaas received from ws")
				break
			}

			log.WithField("message", m.Message).WithField("consumerGroupID", consumerGroupID).
				Debug("Send message to qaas received from ws")

			roomIn <- m.Message

			var cm msg
			if err := json.Unmarshal([]byte(m.Message), &cm); err != nil {
				log.WithError(err).WithField("data", m.Message).Error("Fail to unmarshal JSON")
			}

			now := fmt.Sprintf("%s", time.Now())
			switch {
			case cm.Message == "ping":
				roomIn <- `{"user": "qliws-bot", "message":"pong >` + now + `"}`
			}
		}
	}()

	// Send each message received from kafka to the websocket

	roomOut := qli.Sub()

	go func() {
		for {
			m := <-roomOut

			if err := websocket.JSON.Send(ws, m); err != nil {
				log.WithError(err).Error("Sending message to ws received from qaas")
				break
			}

			log.WithField("message", m).WithField("consumerGroupID", consumerGroupID).
				Debug("Send message to ws received from qaas")
		}
	}()

	select {}
}
