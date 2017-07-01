package main

import (
	"io"
	"strings"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/websocket"
)

// Broadcast messages to all connected websockets
type hub struct {
	topic      string
	conns      map[*websocket.Conn]bool
	unregister chan *websocket.Conn
	register   chan *websocket.Conn
	broadcast  chan []byte
}

func getHub(topic string) *hub {
	h, ok := hubs[topic]
	if !ok {
		h = &hub{
			topic:      topic,
			conns:      make(map[*websocket.Conn]bool),
			register:   make(chan *websocket.Conn),
			broadcast:  make(chan []byte),
			unregister: make(chan *websocket.Conn),
		}
		hubs[topic] = h
		log.WithField("topic", topic).Debug("Run new hub")
		go h.run(topic)
	}

	return h
}

func (h *hub) run(topic string) {
	for {
		select {
		case c := <-h.register:
			log.WithField("topic", topic).Debug("WS registered from the hub")
			h.conns[c] = true

		case c := <-h.unregister:
			log.WithField("topic", topic).Debug("WS unregistered from the hub")
			_, ok := h.conns[c]
			if ok {
				delete(h.conns, c)
			}

		case msg := <-h.broadcast:
			log.WithField("topic", topic).WithField("msg", string(msg)).Debug("Broadcast msg from the hub")
			h.broadcastMsg(msg)
		}
	}
}

func (h *hub) broadcastMsg(msg []byte) {
	log.Debugf("Broadcast msg to %d conns", len(h.conns))
	for ws := range h.conns {
		if err := websocket.Message.Send(ws, string(msg)); err != nil {
			if strings.Contains(err.Error(), "write: broken pipe") || err == io.EOF {
				delete(h.conns, ws)
				ws.Close()
				log.Debug("Force WS unregistered from the hub")
				continue
			}
			log.WithError(err).Error("Fail to send kafka message to ws")
			continue
		}
		log.Debug("Msg send to ws")
	}
}
