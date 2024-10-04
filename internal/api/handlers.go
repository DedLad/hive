package api

import (
	"net/http"
	"os"
	"strings"

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
	router.POST("/put/:key_value", PutHandler)
	router.GET("/get/:key", GetHandler)
	router.DELETE("/delete/:key", DeleteHandler)
	router.POST("/compact", CompactHandler)
}

func PutHandler(c *gin.Context) {
	keyValue := c.Param("key_value")
	parts := strings.SplitN(keyValue, ":", 2)
	if len(parts) != 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input format. Expected /put/{KEY}:{VALUE}"})
		return
	}

	key := parts[0]
	value := parts[1]

	if err := bitcask.Put(key, value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Successfully stored", "key": key, "value": value})
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

func CompactHandler(c *gin.Context) {
	err := bitcask.Compact()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Compaction failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Compaction completed successfully"})
}
