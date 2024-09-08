package api

import (
	"net/http"
	"os"

	"github.com/DedLad/hive/internal/hive"

	"github.com/gin-gonic/gin"
)

var bitcask *hive.Bitcask

func init() {
	storagePath := os.Getenv("STORAGE_PATH")
	if storagePath == "" {
		storagePath = "./data/bitcask.db"
	}

	var err error
	bitcask, err = hive.NewBitcask(storagePath)
	if err != nil {
		panic("Failed to initialize Bitcask: " + err.Error())
	}
}

func RegisterRoutes(router *gin.Engine) {
	router.PUT("/put", PutHandler)
	router.GET("/get/:key", GetHandler)
	router.DELETE("/delete/:key", DeleteHandler)
}

func PutHandler(c *gin.Context) {
	var req map[string]string
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	for key, value := range req {
		if err := bitcask.Put(key, value); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "Successfully stored"})
}

func GetHandler(c *gin.Context) {
	key := c.Param("key")
	value, err := bitcask.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": value})
}

func DeleteHandler(c *gin.Context) {
	key := c.Param("key")
	if err := bitcask.Delete(key); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Successfully deleted"})
}
