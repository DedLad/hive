package main

import (
	"github.com/DedLad/hive/internal/api"

	"github.com/DedLad/hive/config"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	cfg := config.LoadConfig()

	api.RegisterRoutes(router)

	router.Run(cfg.ServerAddress)
}
