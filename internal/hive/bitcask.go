package hive

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type Bitcask struct {
	data       map[string]string
	wal        *WAL
	dbFile     *os.File
	mu         sync.RWMutex // Mutex for concurrent access
	walPath    string       // Path to the WAL file
	dbPath     string       // Path to the DB file
	logCount   int          // Count of logs in the WAL
	compactMux sync.Mutex   // Mutex for compaction
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
			bc.data[key] = "__DELETED__"
		}
		bc.logCount++
	})
	if err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	go bc.periodicCompaction()

	return bc, nil
}

func (bc *Bitcask) periodicCompaction() {
	ticker := time.NewTicker(90 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		bc.Compact()
	}
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
	bc.logCount++
	bc.checkCompaction()
	return nil
}

func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.wal.Append("DELETE", key, ""); err != nil {
		return err
	}

	// Mark the key as a tombstone in the .db file
	if _, err := bc.dbFile.WriteString(fmt.Sprintf("%s:%s\n", key, "__DELETED__")); err != nil {
		return err
	}

	bc.data[key] = "__DELETED__"
	bc.logCount++
	bc.checkCompaction()
	return nil
}

func (bc *Bitcask) Get(key string) (string, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	value, exists := bc.data[key]
	if !exists {
		return "", errors.New("key not found")
	}
	if value == "__DELETED__" {
		return "", errors.New("key not found")
	}
	return value, nil
}

func (bc *Bitcask) Compact() error {
	bc.compactMux.Lock()
	defer bc.compactMux.Unlock()

	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Read the current database file
	data, err := os.ReadFile(bc.dbPath)
	if err != nil {
		return fmt.Errorf("failed to read DB file: %w", err)
	}

	// Use a map to keep track of the latest values for each key
	latestData := make(map[string]string)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]
		latestData[key] = value
	}

	// Filter out tombstones and prepare compacted data
	var compactedData []string
	for key, value := range latestData {
		if value != "__DELETED__" {
			compactedData = append(compactedData, fmt.Sprintf("%s:%s", key, value))
		}
	}

	// Rewrite the filtered data back to the same database file
	err = os.WriteFile(bc.dbPath, []byte(strings.Join(compactedData, "\n")), 0644)
	if err != nil {
		return fmt.Errorf("failed to write compacted data to DB file: %w", err)
	}

	// Reopen the WAL file before logging the compaction operation
	bc.wal, err = NewWAL(bc.walPath)
	if err != nil {
		return fmt.Errorf("failed to initialize new WAL: %w", err)
	}

	// Log the compaction operation after reopening the WAL file
	err = bc.wal.Append("COMPACT", "", "")
	if err != nil {
		return fmt.Errorf("failed to log compaction in WAL: %w", err)
	}

	bc.logCount = 0
	return nil
}

func (bc *Bitcask) checkCompaction() {
	if bc.logCount >= 30 {
		go bc.Compact()
	}
}
