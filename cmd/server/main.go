package main

import (
	"hive/config"
	"hive/internal/api"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	cfg := config.LoadConfig()

	api.RegisterRoutes(router)

	router.Run(cfg.ServerAddress)
}
