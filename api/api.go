package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/thbkrkr/qli/api/controllers"
	"github.com/thbkrkr/qli/client"
	"golang.org/x/net/websocket"
)

var (
	brokers = flag.String("b", "", "Brokers")
	key     = flag.String("k", "", "Key")
	topic   = flag.String("t", "", "Topic")
)

func main() {
	flag.Parse()

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	qli, err := client.NewClient(strings.Split(*brokers, ","), *key, *topic, "qli-api")
	if err != nil {
		log.Error("Fail to create qli client")
		os.Exit(1)
	}

	r.GET("/status", status)
	r.GET("/favicon.ico", favicon)
	r.Static("/0", "./view/")
	r.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/0")
	})

	r.POST("/send", func(c *gin.Context) {
		bytes, _ := ioutil.ReadAll(c.Request.Body)
		data := string(bytes)
		qli.Send(string(data))

		c.JSON(200, gin.H{"produced": data})
	})

	http := controllers.HttpCtrl{Brokers: *brokers}
	r.POST("/produce/topic/:topic", http.Produce)
	r.GET("/consume/topic/:topic", http.Consume)

	/*r.GET("/receive", func(c *gin.Context) {
		data := qli.Receive()
		c.JSON(200, gin.H{"data": data})
	})*/

	ws := controllers.WsCtrl{Brokers: *brokers, Key: *key, Topic: *topic}

	r.GET("/stream", ws.RawStream)
	r.GET("/ws", func(c *gin.Context) {
		handler := websocket.Handler(ws.Consume)
		handler.ServeHTTP(c.Writer, c.Request)
	})

	go qli.CloseOnSig()

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
