package main

import (
	"encoding/json"
	"fmt"
	"io"

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
	defer func() {
		h.unregister <- ws
		ws.Close()
	}()

	// Send each message received from the websocket to Kafka

	kafkaPub, err := ct.Q.PubByTopic(topic)
	if err != nil {
		log.WithError(err).Fatal("Fail to create pub qlient")
		return
	}

	//go func() {
	for {
		var msg Message

		if err := websocket.JSON.Receive(ws, &msg); err != nil {
			if err == io.EOF {
				break
			}

			log.WithError(err).Error("Fail to receive ws json message")
			break
		}
		//WsMsgIn.Mark(1)

		bytes, err := json.Marshal(msg)
		if err != nil {
			log.WithError(err).WithField("message", msg).Error("Fail to marshal JSON")
		}

		kafkaPub <- string(bytes)
		//KafkaMsgIn.Mark(1)

		switch {
		case msg.Message == "ping":
			kafkaPub <- `{"user": "qws~bot-bot1", "message":"pong"}`
			break
		case msg.Message == "who":
			nconn := fmt.Sprintf("%d", len(h.conns))
			kafkaPub <- `{"user": "qws~bot-bot1", "message":"` + nconn + ` connections"}`
			break
		}
	}
	//}()

	// Send each message received from Kafka to the websocket

	/*kafkaSub, err := ct.Q.SubByTopic(topic)
	if err != nil {
		log.WithError(err).Fatal("Fail to create sub qlient")
		return
	}

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

	select {}*/
}

func (ct *WsCtrl) RawStream(c *gin.Context) {
	kafkaSub, err := ct.Q.Sub()
	if err != nil {
		log.WithError(err).Fatal("Fail to create sub qlient")
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.Status(200)
	go func() {
		for event := range kafkaSub {
			_, err := c.Writer.Write([]byte(event + "\n"))
			if err != nil {
				log.WithError(err).Error("Fail to send message")
				break
			}
			c.Writer.Flush()
		}
	}()

	select {}
}
