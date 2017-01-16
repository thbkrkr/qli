package controllers

import (
	"time"

	"github.com/thbkrkr/qli/client"
)

var (
	sessions = map[string]*Session{}
	//mtx      sync.RWMutex
)

type Session struct {
	client   *client.Qlient
	lastPing time.Time
}

/*
func init() {
	go func() {
		for {

			//mtx.Lock()
			logrus.Printf("check %d sessions", len(sessions))
			for topic, session := range sessions {
				logrus.Printf("check if session %s is old", topic)
				if time.Since(session.lastPing) > time.Duration(10*time.Second) {
					session.client.Close()
					logrus.Printf("delete session %s", topic)
					delete(sessions, topic)
				}
			}
			logrus.Printf("ok check %d sessions", len(sessions))
			//mtx.Unlock()
			time.Sleep(5 * time.Second)
		}
	}()
}


func Pub(c *gin.Context) {
	topic := c.Param("topic")
	key := c.Request.Header.Get("X-Auth-Key")

	//var obj interface{}
	//c.BindJSON(&obj)
	data, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(400, gin.H{"message": err.Error()})
		return
	}

	q, err := qlientPerTopic(key, topic)
	if err != nil {
		c.JSON(400, gin.H{"message": err.Error()})
		return
	}

	if q == nil {
		c.JSON(500, gin.H{"message": "error"})
		return
	}

	partition, offset, err := q.Send(string(data))
	if err != nil {
		c.JSON(500, gin.H{"type": "error", "message": err.Error()})
		return
	}

	c.JSON(200, gin.H{"partition": partition, "offset": offset})
}

func Sub(c *gin.Context) {
	topic := c.Param("topic")
	key := c.Request.Header.Get("X-Auth-Key")

	q, err := qlientPerTopic(key, topic)
	if err != nil {
		c.JSON(400, gin.H{"message": err.Error()})
		return
	}

	if q == nil {
		try := 0
		for try != 10 {
			logrus.Info("wait")
			time.Sleep(50 * time.Millisecond)
			try++
		}
	}

	if q == nil {
		c.JSON(500, gin.H{"message": "error"})
		return
	}

	obj := <-q.Sub()

	c.JSON(200, obj)
}

func qlientPerTopic(key string, topic string) (*client.Qlient, error) {
	//mtx.RLock()
	session := sessions[topic]
	//mtx.RUnlock()

	if session == nil {
		newQlient, err := client.NewClientFromEnv("qli-http")

		ClientsHttp.Mark(1)

		if err != nil {
			return nil, err
		}

		session = &Session{
			client:   newQlient,
			lastPing: time.Now(),
		}
		//mtx.Lock()
		sessions[topic] = session
		//mtx.Unlock()
	}

	return session.client, nil
}

*/
