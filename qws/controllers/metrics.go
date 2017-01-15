package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/rcrowley/go-metrics"
)

var (
	KafkaMsgIn  = metrics.GetOrRegisterMeter("kafka-msg-in", metrics.DefaultRegistry)
	WsMsgIn     = metrics.GetOrRegisterMeter("ws-msg-in", metrics.DefaultRegistry)
	KafkaMsgOut = metrics.GetOrRegisterMeter("kafka-msg-out", metrics.DefaultRegistry)
	WsMsgOut    = metrics.GetOrRegisterMeter("ws-msg-out", metrics.DefaultRegistry)

	ClientsWs   = metrics.GetOrRegisterMeter("ws-clients", metrics.DefaultRegistry)
	ClientsHttp = metrics.GetOrRegisterMeter("http-clients", metrics.DefaultRegistry)
)

func Metrics(c *gin.Context) {
	c.JSON(200, gin.H{
		"kafka-msg-in":  KafkaMsgIn,
		"ws-msg-in":     WsMsgIn,
		"kafka-msg-out": KafkaMsgOut,
		"ws-msg-out":    WsMsgOut,
		"ws-clients":    ClientsWs,
		"http-clients":  ClientsHttp,
	})
}
