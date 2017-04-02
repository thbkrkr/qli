package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

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
