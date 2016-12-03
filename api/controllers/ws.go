package controllers

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/thbkrkr/qli/client"
	"golang.org/x/net/websocket"
)

type WsCtrl struct {
	Q *client.Qlient
}

type Message struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func newQlient() (*client.Qlient, error) {
	q, err := client.NewClientFromEnv(fmt.Sprintf("qli-ws-%d", time.Now().Unix()))
	if err != nil {
		ClientsWs.Mark(1)
	}
	return q, err
}

func (ct *WsCtrl) RawStream(c *gin.Context) {
	roomOut := ct.Q.Sub()

	c.Status(200)

	go func() {
		for {
			m := <-roomOut
			_, err := c.Writer.Write([]byte(m + "\n"))
			if err != nil {
				log.WithError(err).Error("Fail to send ws message")
				break
			}
			c.Writer.Flush()
		}
	}()

	select {}
}

func (ct *WsCtrl) Consume(ws *websocket.Conn) {
	q, err := newQlient()
	if err != nil {
		log.WithError(err).Fatal("Fail to create qlient")
		return
	}

	// Send each message received from the websocket to Kafka

	kafkaPub, err := q.Pub()
	if err != nil {
		log.WithError(err).Fatal("Fail to create qlient")
		return
	}

	go func() {
		for {
			var msg Message

			if err := websocket.JSON.Receive(ws, &msg); err != nil {
				log.WithError(err).Error("Fail to receive ws json message")
				break
			}

			WsMsgIn.Mark(1)

			bytes, err := json.Marshal(msg)
			if err != nil {
				log.WithError(err).WithField("message", msg).Error("Fail to marshal JSON")
			}

			kafkaPub <- string(bytes)

			KafkaMsgIn.Mark(1)

			now := fmt.Sprintf("%s", time.Now())
			switch {
			case msg.Message == "ping":
				kafkaPub <- `{"user": "qliws-bot", "message":"pong >` + now + `"}`
			}
		}
	}()

	// Send each message received from Kafka to the websocket

	kafkaSub := q.Sub()

	go func() {
		for {
			m := <-kafkaSub

			KafkaMsgOut.Mark(1)

			if err := websocket.JSON.Send(ws, m); err != nil {
				q.Close()
				if strings.Contains(err.Error(), "write: broken pipe") ||
					err == io.EOF {
					break
				}
				log.WithError(err).Error("Sending message to ws received from kafka")
				break
			}

			WsMsgOut.Mark(1)

		}
	}()

	select {}
}
