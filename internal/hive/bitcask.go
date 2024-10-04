package hive

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Bitcask struct {
	data    map[string]string
	wal     *WAL
	mu      sync.RWMutex // Mutex for concurrent access
	walPath string       // Path to the WAL file
}

func NewBitcask(walPath string) (*Bitcask, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	bc := &Bitcask{
		data:    make(map[string]string),
		wal:     wal,
		walPath: walPath,
	}

	err = wal.Replay(func(operation, key, value string, timestamp int64) {
		if operation == "PUT" {
			bc.data[key] = value
		} else if operation == "DELETE" {
			delete(bc.data, key)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	return bc, nil
}

func (bc *Bitcask) Put(key, value string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.wal.Append("PUT", key, value); err != nil {
		return err
	}

	bc.data[key] = value
	return nil
}

func (bc *Bitcask) Get(key string) (string, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	value, exists := bc.data[key]
	if !exists {
		return "", errors.New("key not found")
	}
	return value, nil
}

func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	delete(bc.data, key)

	if err := bc.wal.Append("DELETE", key, ""); err != nil {
		return err
	}

	return nil
}

func (bc *Bitcask) Compact() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	err := bc.wal.Close()
	if err != nil {
		return fmt.Errorf("failed to close WAL before compaction: %w", err)
	}

	tempFilePath := "./data/temp_compacted.db"
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	for key, value := range bc.data {
		if value != "__DELETED__" {
			_, err := tempFile.WriteString(fmt.Sprintf("PUT|%s|%s|%d\n", key, value, time.Now().Unix()))
			if err != nil {
				return fmt.Errorf("failed to write to temp file: %w", err)
			}
		}
	}

	dataFiles, err := filepath.Glob("./data/bitcask*.db")
	if err != nil {
		return fmt.Errorf("failed to find data files: %w", err)
	}

	for _, file := range dataFiles {
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("failed to remove old data file %s: %w", file, err)
		}
	}

	err = os.Rename(tempFilePath, "./data/bitcask.db")
	if err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}
