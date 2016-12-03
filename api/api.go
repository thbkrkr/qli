package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/thbkrkr/qli/api/controllers"
	"github.com/thbkrkr/qli/client"
	"golang.org/x/net/websocket"
)

func main() {
	log.SetLevel(log.DebugLevel)

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.GET("/status", status)
	r.GET("/favicon.ico", favicon)
	r.Static("/0", "./view/")
	r.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/0")
	})

	q, err := client.NewClientFromEnv("qli-ws-main")
	if err != nil {
		fmt.Printf("%s: %s\n", "Fail to create qlient", err.Error())
		os.Exit(1)
	}

	r.POST("/send", func(c *gin.Context) {
		bytes, _ := ioutil.ReadAll(c.Request.Body)
		data := string(bytes)
		q.Send(string(data))

		c.JSON(200, gin.H{"produced": data})
	})

	r.POST("/produce/topic/:topic", controllers.Produce)
	r.GET("/consume/topic/:topic", controllers.Consume)
	r.GET("/metrics", controllers.Metrics)

	/*r.GET("/receive", func(c *gin.Context) {
		data := qli.Receive()
		c.JSON(200, gin.H{"data": data})
	})*/

	ws := controllers.WsCtrl{q}

	r.GET("/stream", ws.RawStream)
	r.GET("/ws", func(c *gin.Context) {
		handler := websocket.Handler(ws.Consume)
		handler.ServeHTTP(c.Writer, c.Request)
	})

	go q.CloseOnSig()

	log.WithField("port", 4242).Info("Start qli-ws")
	r.Run(":4242")
}

func status(c *gin.Context) {
	c.JSON(200, gin.H{
		"name":   "qli-api",
		"ok":     "true",
		"status": 200,
	})
}

func favicon(c *gin.Context) {
	c.JSON(200, nil)
}
