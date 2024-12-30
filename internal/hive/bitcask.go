package hive

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	dataFileSizeThreshold = 100
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
		if err := bc.loadKeyDir(f, len(bc.dataFiles)-1); err != nil {
			return err
		}
	}
	// Create or open the active file (last index)
	activeFile, err := os.OpenFile(filepath.Join(bc.dataDir,
		fmt.Sprintf("%d.data", len(bc.dataFiles))), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	bc.activeFile = activeFile
	bc.dataFiles = append(bc.dataFiles, activeFile)
	return nil
}

func (bc *Bitcask) loadKeyDir(file *os.File, fileID int) error {
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
			FileID:    fileID,
			Offset:    offset,
			Timestamp: timestamp,
		}
		offset += int64(len(line) + 1)
	}
	return scanner.Err()
}

func (bc *Bitcask) rotateActiveFile() error {
	// if err := bc.activeFile.Close(); err != nil {
	// 	return err
	// }
	newFile, err := os.OpenFile(filepath.Join(bc.dataDir,
		fmt.Sprintf("%d.data", len(bc.dataFiles))), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	bc.activeFile = newFile
	bc.dataFiles = append(bc.dataFiles, newFile)
	return nil
}

func (bc *Bitcask) Put(key, value string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	timestamp := time.Now().Unix()
	entry := fmt.Sprintf("%s|%s|%d|%d|%d\n", key, value, len(key), len(value), timestamp)
	if _, err := bc.activeFile.WriteString(entry); err != nil {
		return err
	}
	offset, err := bc.activeFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	bc.keyDir[key] = KeyDirEntry{
		FileID:    len(bc.dataFiles) - 1,
		Offset:    offset - int64(len(entry)),
		Timestamp: timestamp,
	}

	// Just rotate the active file if needed. No automatic compaction here.
	if offset >= dataFileSizeThreshold {
		if err := bc.rotateActiveFile(); err != nil {
			return err
		}
	}
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
	if _, err := file.Seek(entry.Offset, io.SeekStart); err != nil {
		return "", err
	}
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
	return parts[1], nil
}

func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	timestamp := time.Now().Unix()
	entry := fmt.Sprintf("%s|%s|%d|%d|%d\n", key, "TOMBSTONE", len(key), 0, timestamp)
	if _, err := bc.activeFile.WriteString(entry); err != nil {
		return err
	}
	offset, err := bc.activeFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// Remove from keyDir since it's tombstoned
	delete(bc.keyDir, key)
	// Make a new entry for the tombstone so we don't resurrect it during compaction
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
	return nil
}
func (bc *Bitcask) Compact() error {
	bc.compactMux.Lock()
	defer bc.compactMux.Unlock()
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if len(bc.dataFiles) <= 2 {
		return nil
	}

	compactedIndex := 0
	for {
		name := filepath.Join(bc.dataDir, fmt.Sprintf("compacted%d.data", compactedIndex))
		if _, err := os.Stat(name); os.IsNotExist(err) {
			break
		}
		compactedIndex++
	}

	compactedFileName := filepath.Join(bc.dataDir, fmt.Sprintf("compacted%d.data", compactedIndex))
	compactedFile, err := os.OpenFile(compactedFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	inactiveFiles := make([]*os.File, len(bc.dataFiles)-1)
	copy(inactiveFiles, bc.dataFiles[:len(bc.dataFiles)-1])

	latestEntries := make(map[string]struct {
		entry KeyDirEntry
		line  string
	})

	for fileID, file := range inactiveFiles {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			compactedFile.Close()
			return err
		}

		scanner := bufio.NewScanner(file)
		var offset int64
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "|")
			if len(parts) != 5 {
				offset += int64(len(line) + 1)
				continue
			}

			key := parts[0]
			ts, _ := strconv.ParseInt(parts[4], 10, 64)

			if existing, ok := latestEntries[key]; !ok || ts > existing.entry.Timestamp {
				latestEntries[key] = struct {
					entry KeyDirEntry
					line  string
				}{
					entry: KeyDirEntry{
						FileID:    fileID,
						Offset:    offset,
						Timestamp: ts,
					},
					line: line + "\n",
				}
			}
			offset += int64(len(line) + 1)
		}
	}

	var newOffset int64
	for key, data := range latestEntries {
		if strings.Contains(data.line, "|TOMBSTONE|") {
			continue
		}
		if _, err := compactedFile.WriteString(data.line); err != nil {
			compactedFile.Close()
			return err
		}
		bc.keyDir[key] = KeyDirEntry{
			FileID:    0,
			Offset:    newOffset,
			Timestamp: data.entry.Timestamp,
		}
		newOffset += int64(len(data.line))
	}

	newDataFiles := make([]*os.File, 0)

	compactedHandle, err := os.OpenFile(compactedFileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	newDataFiles = append(newDataFiles, compactedHandle)

	activeFile := bc.dataFiles[len(bc.dataFiles)-1]
	newDataFiles = append(newDataFiles, activeFile)

	// Close and remove old files
	for _, file := range inactiveFiles {
		filename := file.Name()
		file.Close()
		os.Remove(filename)
	}
	// inactiveFiles = append(inactiveFiles, compactedHandle)
	bc.dataFiles = newDataFiles

	bc.keyDir = make(map[string]KeyDirEntry)
	for i, f := range bc.dataFiles {
		if err := bc.loadKeyDir(f, i); err != nil {
			return err
		}
	}

	return nil
}
