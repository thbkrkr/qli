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

func (ct *WsCtrl) WsPub(ws *websocket.Conn, topic string) {
	log.WithField("topic", topic).Info("register ws in hub")
	h := getHub(topic)
	h.register <- ws

	defer func() {
		h.unregister <- ws
		ws.Close()
	}()

	// Send each message received from the websocket to Kafka

	log.WithField("topic", topic).Info("create pub kafka for receive ws")
	kafkaPub, err := ct.Q.PubOn(topic)
	if err != nil {
		log.WithError(err).Fatal("Fail to create pub qlient")
		return
	}

	for {
		var msg Message

		if err := websocket.JSON.Receive(ws, &msg); err != nil {
			if err == io.EOF {
				break
			}

			log.WithError(err).Error("Fail to receive ws json message")
			break
		}

		bytes, err := json.Marshal(msg)
		if err != nil {
			log.WithError(err).WithField("message", msg).Error("Fail to marshal JSON")
		}

		log.WithField("topic", topic).Info("pub new message in kafka")

		kafkaPub <- bytes

		// Default commands
		switch {
		case msg.Message == "ping":
			kafkaPub <- []byte(`{"user": "qws~bot-bot1", "message":"pong"}`)
			break
		case msg.Message == "who":
			nconn := fmt.Sprintf("%d", len(h.conns))
			kafkaPub <- []byte(`{"user": "qws~bot-bot1", "message":"` + nconn + ` connections"}`)
			break
		}
	}

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
	topic := c.Param("topic")

	kafkaSub, err := ct.Q.SubOn(topic)
	if err != nil {
		log.WithError(err).Fatal("Fail to create sub qlient")
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.Status(200)
	go func() {
		for event := range kafkaSub {
			_, err := c.Writer.Write(append(event, []byte("\n")...))
			if err != nil {
				log.WithError(err).Error("Fail to send message")
				break
			}
			c.Writer.Flush()
		}
	}()

	select {}
}
