package hive

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Bitcask struct {
	mu      sync.RWMutex
	storage map[string]string
	file    *os.File
}

func NewBitcask(filePath string) (*Bitcask, error) {
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %v", err)
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	bc := &Bitcask{
		storage: make(map[string]string),
		file:    file,
	}
	if err := bc.loadFromFile(); err != nil {
		return nil, err
	}
	return bc, nil
}

func (bc *Bitcask) loadFromFile() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	fileInfo, err := bc.file.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size() == 0 {
		return nil
	}
	//TODO: Implement logic to read data from the file and populate bc.storage

	return nil
}

func (bc *Bitcask) Put(key, value string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.storage[key] = value
	if _, err := bc.file.WriteString(fmt.Sprintf("%s=%s\n", key, value)); err != nil {
		return err
	}

	return nil
}

func (bc *Bitcask) Get(key string) (string, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if val, exists := bc.storage[key]; exists {
		return val, nil
	}
	return "", errors.New("key not found")
}

func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if _, exists := bc.storage[key]; !exists {
		return errors.New("key not found")
	}
	delete(bc.storage, key)
	//TODO: Implement logic to mark key as deleted in the file, maybe tombstone have to look into it

	return nil
}

func (bc *Bitcask) Close() error {
	return bc.file.Close()
}
