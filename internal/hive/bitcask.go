package hive

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	dataFileSizeThreshold   = 100 // 100 bytes
	compactionFileThreshold = 4
)

type KeyDirEntry struct {
	FileID    int
	Offset    int64
	Timestamp int64
}

type Bitcask struct {
	dataDir    string
	keyDir     map[string]KeyDirEntry
	dataFiles  []*os.File
	activeFile *os.File
	mu         sync.RWMutex
	compactMux sync.Mutex
}

func NewBitcask(dataDir string) (*Bitcask, error) {
	bc := &Bitcask{
		dataDir: dataDir,
		keyDir:  make(map[string]KeyDirEntry),
	}

	if err := bc.loadDataFiles(); err != nil {
		return nil, fmt.Errorf("failed to load data files: %w", err)
	}

	return bc, nil
}

func (bc *Bitcask) loadDataFiles() error {
	files, err := filepath.Glob(filepath.Join(bc.dataDir, "*.data"))
	if err != nil {
		return err
	}

	for _, file := range files {
		f, err := os.OpenFile(file, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		bc.dataFiles = append(bc.dataFiles, f)
		if err := bc.loadKeyDir(f); err != nil {
			return err
		}
	}

	activeFile, err := os.OpenFile(filepath.Join(bc.dataDir, fmt.Sprintf("%d.data", len(bc.dataFiles))), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	bc.activeFile = activeFile
	bc.dataFiles = append(bc.dataFiles, activeFile)

	return nil
}

func (bc *Bitcask) loadKeyDir(file *os.File) error {
	scanner := bufio.NewScanner(file)
	var offset int64
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "|")
		if len(parts) != 5 {
			continue
		}

		timestamp, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			return err
		}

		key := parts[0]
		bc.keyDir[key] = KeyDirEntry{
			FileID:    len(bc.dataFiles) - 1,
			Offset:    offset,
			Timestamp: timestamp,
		}
		offset += int64(len(line) + 1)
	}
	return scanner.Err()
}

func (bc *Bitcask) Put(key, value string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	timestamp := time.Now().Unix()
	entry := fmt.Sprintf("%s|%s|%d|%d|%d\n", key, value, len(key), len(value), timestamp)
	if _, err := bc.activeFile.WriteString(entry); err != nil {
		return err
	}

	offset, err := bc.activeFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	bc.keyDir[key] = KeyDirEntry{
		FileID:    len(bc.dataFiles) - 1,
		Offset:    offset - int64(len(entry)),
		Timestamp: timestamp,
	}

	if offset >= dataFileSizeThreshold {
		if err := bc.rotateActiveFile(); err != nil {
			return err
		}
	}

	if len(bc.dataFiles) > compactionFileThreshold {
		go bc.Compact()
	}

	return nil
}

func (bc *Bitcask) rotateActiveFile() error {
	bc.activeFile.Close()
	newFile, err := os.OpenFile(filepath.Join(bc.dataDir, fmt.Sprintf("%d.data", len(bc.dataFiles))), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	bc.activeFile = newFile
	bc.dataFiles = append(bc.dataFiles, newFile)
	return nil
}

func (bc *Bitcask) Get(key string) (string, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	entry, exists := bc.keyDir[key]
	if !exists {
		return "", errors.New("key not found")
	}

	if entry.FileID < 0 || entry.FileID >= len(bc.dataFiles) {
		return "", errors.New("invalid file ID")
	}

	file := bc.dataFiles[entry.FileID]
	file.Seek(entry.Offset, os.SEEK_SET)

	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	parts := strings.Split(line, "|")
	if len(parts) != 5 {
		return "", errors.New("invalid data format")
	}

	if parts[1] == "TOMBSTONE" {
		return "", errors.New("key not found")
	}

	value := parts[1]
	return value, nil
}

func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	timestamp := time.Now().Unix()
	entry := fmt.Sprintf("%s|%s|%d|%d|%d\n", key, "TOMBSTONE", len(key), 0, timestamp)
	if _, err := bc.activeFile.WriteString(entry); err != nil {
		return err
	}

	delete(bc.keyDir, key)

	offset, err := bc.activeFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	bc.keyDir[key] = KeyDirEntry{
		FileID:    len(bc.dataFiles) - 1,
		Offset:    offset - int64(len(entry)),
		Timestamp: timestamp,
	}

	if offset >= dataFileSizeThreshold {
		if err := bc.rotateActiveFile(); err != nil {
			return err
		}
	}

	if len(bc.dataFiles) > compactionFileThreshold {
		go bc.Compact()
	}

	return nil
}

func (bc *Bitcask) Compact() error {
	bc.compactMux.Lock()
	defer bc.compactMux.Unlock()

	bc.mu.Lock()
	defer bc.mu.Unlock()

	compactedFileIndex := 0
	for {
		if _, err := os.Stat(filepath.Join(bc.dataDir, fmt.Sprintf("compacted%d.data", compactedFileIndex))); os.IsNotExist(err) {
			break
		}
		compactedFileIndex++
	}
	compactedFileName := filepath.Join(bc.dataDir, fmt.Sprintf("compacted%d.data", compactedFileIndex))

	compactedFile, err := os.OpenFile(compactedFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer compactedFile.Close()

	for key, entry := range bc.keyDir {
		if entry.FileID >= len(bc.dataFiles) {
			continue
		}
		file := bc.dataFiles[entry.FileID]
		file.Seek(entry.Offset, os.SEEK_SET)

		reader := bufio.NewReader(file)
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		parts := strings.Split(line, "|")
		if len(parts) != 5 {
			return errors.New("invalid data format")
		}

		if parts[1] == "TOMBSTONE" {
			continue
		}

		if _, err := compactedFile.WriteString(line); err != nil {
			return err
		}

		offset, err := compactedFile.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		bc.keyDir[key] = KeyDirEntry{
			FileID:    len(bc.dataFiles),
			Offset:    offset - int64(len(line)),
			Timestamp: entry.Timestamp,
		}
	}

	for _, file := range bc.dataFiles {
		file.Close()
		os.Remove(file.Name())
	}

	compactedFile, err = os.OpenFile(compactedFileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	bc.dataFiles = []*os.File{compactedFile}
	bc.activeFile = compactedFile

	if err := bc.rotateActiveFile(); err != nil {
		return err
	}

	return nil
}
