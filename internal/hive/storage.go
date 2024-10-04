package hive

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type WAL struct {
	file *os.File
}

func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	return &WAL{file: file}, nil
}

func (wal *WAL) Append(operation, key, value string) error {
	timestamp := time.Now().Unix()
	_, err := wal.file.WriteString(fmt.Sprintf("%s|%s|%s|%d\n", operation, key, value, timestamp))
	return err
}

func (wal *WAL) Replay(callback func(operation, key, value string, timestamp int64)) error {
	wal.file.Seek(0, 0)
	scanner := bufio.NewScanner(wal.file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "|", 4)
		if len(parts) == 4 {
			timestamp, err := strconv.ParseInt(parts[3], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid timestamp in WAL: %w", err)
			}
			callback(parts[0], parts[1], parts[2], timestamp)
		}
	}
	return scanner.Err()
}

func (wal *WAL) Close() error {
	return wal.file.Close()
}
func (bc *Bitcask) ArchiveWAL() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	err := bc.wal.Close()
	if err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	archivePath := bc.walPath + ".archive"
	err = os.Rename(bc.walPath, archivePath)
	if err != nil {
		return fmt.Errorf("failed to archive WAL: %w", err)
	}

	bc.wal, err = NewWAL(bc.walPath)
	if err != nil {
		return fmt.Errorf("failed to initialize new WAL: %w", err)
	}

	return nil
}
