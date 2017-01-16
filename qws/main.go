package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/thbkrkr/go-utilz/http"
	"github.com/thbkrkr/qli/client"
	"github.com/thbkrkr/qli/qws/controllers"
	"golang.org/x/net/websocket"
)

var (
	name      = "qws"
	buildDate = "dev"
	gitCommit = "dev"
	port      = 4242

	h hub
)

func main() {
	hostname, _ := os.Hostname()

	q, err := client.NewClientFromEnv(fmt.Sprintf("qws-%s", hostname))
	if err != nil {
		log.WithError(err).Error("Fail to create qlient")
		os.Exit(1)
	}

	h = hub{
		conns:      make(map[*websocket.Conn]bool),
		register:   make(chan *websocket.Conn),
		broadcast:  make(chan string),
		unregister: make(chan *websocket.Conn),
	}
	go h.run()

	topic := "thbkrkr.miaou"
	sub, err := q.SubByTopic(topic)
	if err != nil {
		log.WithError(err).Error("Fail to create sub qlient")
		os.Exit(1)
	}

	go func() {
		for event := range sub {
			log.WithField("event", event).Info("Event")
			h.broadcast <- event
		}
	}()

	go http.API(name, buildDate, gitCommit, port, func(r *gin.Engine) {
		ctl := WsCtrl{Q: q}

		r.GET("/ws", func(c *gin.Context) {
			topic := c.Query("topic")
			if topic == "" {
				c.JSON(400, "Invalid topic")
				return
			}

			log.WithField("topic", topic).Info("Start WS")
			handler := websocket.Handler(func(ws *websocket.Conn) {
				h.register <- ws
				ctl.Consume(ws, topic)
			})
			handler.ServeHTTP(c.Writer, c.Request)
		})

		r.GET("/stream", ctl.RawStream)

		/*
			r.POST("/pub/topic/:topic", controllers.Produce)
			r.GET("/sub/topic/:topic", controllers.Consume)
			r.POST("/send", func(c *gin.Context) {
				bytes, _ := ioutil.ReadAll(c.Request.Body)
				data := string(bytes)
				q.Send(string(data))
				c.JSON(200, gin.H{"produced": data})
			})
			/*r.GET("/receive", func(c *gin.Context) {
			        data := qli.Receive()
			        c.JSON(200, gin.H{"data": data})
			})*/

		r.GET("/metrics", controllers.Metrics)
	})

	q.CloseOnSig()
}

type hub struct {
	conns      map[*websocket.Conn]bool
	broadcast  chan string
	unregister chan *websocket.Conn
	register   chan *websocket.Conn
	//content string
}

func (h *hub) run() {
	for {
		select {
		case c := <-h.register:
			h.conns[c] = true

		case c := <-h.unregister:
			log.Info("hub unregister")
			_, ok := h.conns[c]
			if ok {
				delete(h.conns, c)
			}

		case event := <-h.broadcast:
			//KafkaMsgOut.Mark(1)
			h.broadcastEvent(event)
		}
	}
}

func (h *hub) broadcastEvent(event string) {
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
