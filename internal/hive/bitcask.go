package hive

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

type Bitcask struct {
	data    map[string]string
	wal     *WAL
	dbFile  *os.File
	mu      sync.RWMutex // Mutex for concurrent access
	walPath string       // Path to the WAL file
	dbPath  string       // Path to the DB file
}

func NewBitcask(walPath, dbPath string) (*Bitcask, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	dbFile, err := os.OpenFile(dbPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB file: %w", err)
	}

	bc := &Bitcask{
		data:    make(map[string]string),
		wal:     wal,
		dbFile:  dbFile,
		walPath: walPath,
		dbPath:  dbPath,
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

	if _, err := bc.dbFile.WriteString(fmt.Sprintf("%s:%s\n", key, value)); err != nil {
		return err
	}

	bc.data[key] = value
	return nil
}

func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.wal.Append("DELETE", key, ""); err != nil {
		return err
	}

	if _, err := bc.dbFile.WriteString(fmt.Sprintf("%s:%s\n", key, "__DELETED__")); err != nil {
		return err
	}

	delete(bc.data, key)
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
			_, err := tempFile.WriteString(fmt.Sprintf("%s:%s\n", key, value))
			if err != nil {
				return fmt.Errorf("failed to write to temp file: %w", err)
			}
		}
	}

	err = os.Rename(tempFilePath, bc.dbPath)
	if err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}
