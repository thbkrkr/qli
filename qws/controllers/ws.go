package controllers

import (
	"encoding/json"
	"io"
	"strings"

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

func (ct *WsCtrl) Consume(ws *websocket.Conn, topic string) {

	// Send each message received from the websocket to Kafka

	kafkaPub, err := ct.Q.PubByTopic(topic)
	if err != nil {
		log.WithError(err).Fatal("Fail to create qlient")
		return
	}

	go func() {
		for {
			var msg Message

			if err := websocket.JSON.Receive(ws, &msg); err != nil {
				if err == io.EOF {
					//ct.Q.Close()
					break
				}
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

			switch {
			case msg.Message == "ping":
				kafkaPub <- `{"user": "qws~bot-bot1", "message":"pong"}`
			}
		}
	}()

	// Send each message received from Kafka to the websocket

	kafkaSub := ct.Q.SubByTopic(topic)

	go func() {
		for m := range kafkaSub {
			KafkaMsgOut.Mark(1)

			if err := websocket.JSON.Send(ws, m); err != nil {
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
