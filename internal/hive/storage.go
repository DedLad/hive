package hive

import (
	"fmt"
	"os"
	"time"
)

type WAL struct {
	file *os.File
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

func (wal *WAL) Append(operation, key, value string) error {
	timestamp := time.Now().Unix()
	logEntry := fmt.Sprintf("%s|%s|%s|%d\n", operation, key, value, timestamp)
	_, err := wal.file.WriteString(logEntry)
	return err
}

func (wal *WAL) Close() error {
	return wal.file.Close()
}

func (wal *WAL) Replay(callback func(operation, key, value string, timestamp int64)) error {
	file, err := os.Open(wal.file.Name())
	if err != nil {
		return err
	}
	defer file.Close()

	var operation, key, value string
	var timestamp int64
	for {
		_, err := fmt.Fscanf(file, "%s|%s|%s|%d\n", &operation, &key, &value, &timestamp)
		if err != nil {
			break
		}
		callback(operation, key, value, timestamp)
	}
	return nil
}
