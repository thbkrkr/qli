package main

import (
	"fmt"
	"os"

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
)

func main() {
	hostname, _ := os.Hostname()

	q, err := client.NewClientFromEnv(fmt.Sprintf("qws-%s", hostname))
	if err != nil {
		log.WithError(err).Error("Fail to create qlient")
		os.Exit(1)
	}
	go q.CloseOnSig()

	http.API(name, buildDate, gitCommit, port, func(r *gin.Engine) {
		ctl := controllers.WsCtrl{q}

		r.GET("/ws", func(c *gin.Context) {
			topic := c.Query("topic")
			if topic == "" {
				c.JSON(400, "Invalid topic")
				return
			}

			log.WithField("topic", topic).Info("Start WS")
			handler := websocket.Handler(func(ws *websocket.Conn) {
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
}
