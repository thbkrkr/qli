package main

import (
	"encoding/json"
	"fmt"
	"io"

	log "github.com/Sirupsen/logrus"
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
	log.WithField("topic", topic).Debug("Register new ws hub")
	h := getHub(topic)
	h.register <- ws

	defer func() {
		h.unregister <- ws
		ws.Close()
	}()

	// Send each message received from the websocket to Kafka

	log.WithField("topic", topic).Debug("Create pub to forward ws to kafka")

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
			continue
		}

		bytes, err := json.Marshal(msg)
		if err != nil {
			log.WithError(err).WithField("message", msg).Error("Fail to marshal JSON")
			continue
		}

		kafkaPub <- bytes
		log.WithField("topic", topic).Debug("Pub kafka message")

		// Default commands
		switch {
		case msg.Message == "ping":
			kafkaPub <- []byte(`{"user": "qws~bot-bot1", "message":"pong"}`)
			continue
		case msg.Message == "who":
			nconn := fmt.Sprintf("%d", len(h.conns))
			info := "<br><ul>"
			for conn := range h.conns {
				info += "<li>" + conn.Request().Header.Get("User-Agent") + "</li>"
			}
			info += "</ul>"
			kafkaPub <- []byte(`{"user": "qws~bot-bot1", "message":"There is ` + nconn + ` connections: ` + info + `"}`)
			continue
		}
	}
}
