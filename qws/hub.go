package main

import (
	"io"
	"strings"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/websocket"
)

type hub struct {
	topic      string
	conns      map[*websocket.Conn]bool
	broadcast  chan []byte
	unregister chan *websocket.Conn
	register   chan *websocket.Conn
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
		log.WithField("topic", topic).Info("run hub")
		go h.run(topic)
	}

	return h
}

func (h *hub) run(topic string) {
	for {
		select {
		case c := <-h.register:
			log.WithField("topic", topic).Info("hub unregister")
			h.conns[c] = true

		case c := <-h.unregister:
			log.WithField("topic", topic).Info("hub unregister")
			_, ok := h.conns[c]
			if ok {
				delete(h.conns, c)
			}

		case event := <-h.broadcast:
			log.WithField("topic", topic).Info("hub broadcast")
			//KafkaMsgOut.Mark(1)
			h.broadcastEvent(event)
		}
	}
}

func (h *hub) broadcastEvent(event []byte) {
	log.Debugf("Broadcast events to %d conns", len(h.conns))
	for ws := range h.conns {
		if err := websocket.JSON.Send(ws, event); err != nil {
			if strings.Contains(err.Error(), "write: broken pipe") || err == io.EOF {
				delete(h.conns, ws)
				ws.Close()
				continue
			}
			log.WithError(err).Error("Sending message to ws received from kafka")
			continue
		}
		//WsMsgOut.Mark(1)

		/*select {
		  case c.send <- []byte(event):
		    break

		  // We can't reach the client
		  default:
		    close(c.send)
		    delete(h.clients, c)
		  }*/
	}
}
