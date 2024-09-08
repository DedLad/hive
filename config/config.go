package config

import (
	"os"
)

type Config struct {
	ServerAddress string
	StoragePath   string
}

func LoadConfig() *Config {
	serverAddress := getEnv("SERVER_ADDRESS", ":8080")
	storagePath := getEnv("STORAGE_PATH", "./data")
	return &Config{
		ServerAddress: serverAddress,
		StoragePath:   storagePath,
	}
}

func getEnv(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}
