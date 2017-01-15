package http

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
)

// API provides an HTTP API based gin-gonic with cors and base routes
func API(name string, buildDate string, gitCommit string, port int, router func(r *gin.Engine)) {
	var start time.Time

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.Use(cORSMiddleware())

	// Serve _static directory if present
	if _, err := os.Stat("./_static"); !os.IsNotExist(err) {
		r.Static("/s", "./_static/")
		r.GET("/", func(c *gin.Context) {
			c.Redirect(http.StatusMovedPermanently, "/s")
		})
	} else {
		r.GET("/", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"name":   name,
				"ok":     "true",
				"status": 200,
			})
		})
	}

	r.GET("/status", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"buildDate": buildDate,
			"gitCommit": gitCommit,
			"name":      name,
			"uptime":    time.Since(start),
			"ok":        "true",
			"status":    200,
		})
	})

	r.GET("/favicon.ico", func(c *gin.Context) {
		c.JSON(200, nil)
	})

	router(r)

	logrus.WithFields(logrus.Fields{
		"name":      name,
		"buildDate": buildDate,
		"gitCommit": gitCommit,
		"port":      port,
	}).Info("API started")

	start = time.Now()

	err := r.Run(fmt.Sprintf(":%d", port))
	if err != nil {
		logrus.Fatal(err)
	}
}

func cORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		domain := "*"
		c.Writer.Header().Set("Access-Control-Allow-Origin", domain)
		c.Writer.Header().Set("Access-Control-Max-Age", "86400")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, UPDATE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Content-Length")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(200)
		} else {
			c.Next()
		}
	}
}

func JWTAuthMiddleware(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		rawToken := c.Request.Header.Get("X-Auth")

		if rawToken == "" {
			c.AbortWithError(401, errors.New("Authentication failed"))
			return
		}

		token, err := jwt.Parse(rawToken, func(t *jwt.Token) (interface{}, error) {
			b := ([]byte(secret))
			return b, nil
		})

		if err != nil {
			c.AbortWithError(401, err)
		} else if token == nil {
			c.AbortWithError(401, err)
		} else {

			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok || !token.Valid {
				c.AbortWithError(401, err)
			}

			c.Set("AuthID", claims["ID"])
		}
	}
}
