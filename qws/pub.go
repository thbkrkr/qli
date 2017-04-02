package main

import (
	"io/ioutil"

	"github.com/gin-gonic/gin"
)

func Pub(c *gin.Context) {
	topic := c.Param("topic")

	data, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(400, gin.H{"message": err.Error()})
		return
	}

	pub, err := q.AsyncPubOn(topic)
	if err != nil {
		c.JSON(500, gin.H{"message": err.Error()})
		return
	}

	pub <- data

	c.JSON(200, gin.H{"success": true})
}
