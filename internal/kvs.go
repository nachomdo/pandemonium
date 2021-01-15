package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"pingcap.com/kvs/internal/segments"
)

const (
	lockFilename = ".locked"
)

var (
	errInvalidKey             = errors.New("error due to invalid key")
	errDeletingNonExistingKey = errors.New("error removing a key not present in the database")
)

type KVStore interface {
	io.Closer

	// Set the value of a string key to a string
	Set(key string, value []byte) error

	// Get the string value of the a string key. If the key does not exist, return nil.
	Get(key string) ([]byte, bool, error)

	// Remove a given key
	Remove(key string) error
}

type BitCaskStore struct {
	logStore         LogStorage
	basePath         string
	lockFile         *os.File
	hashTable        segments.KeyDirTable
	logCleanerCancel context.CancelFunc
	mutex            *sync.RWMutex
}

func OpenBitCaskStore(path string) (*BitCaskStore, error) {
	// Try to lock the folder
	lockFile, err := os.OpenFile(fmt.Sprintf("%s/%s", path, lockFilename), os.O_RDONLY|os.O_CREATE, 0)

	if err != nil {
		return nil, errors.New("error locking folder for kv store")
	}

	logStore, err := NewLogBasedStorage(path)
	if err != nil {
		return nil, err
	}

	hashTable, err := logStore.BuildKeyDirTable()
	if err != nil {
		return nil, err
	}
	mutex := sync.RWMutex{}
	logCleaner := NewLogCleanerWithPolicy(path, &mutex, hashTable, CleanNonUsed)
	ctx, cancelCleaner := context.WithCancel(context.Background())
	logCleaner.Clean(&ctx)
	return &BitCaskStore{
		basePath:         path,
		logStore:         logStore,
		lockFile:         lockFile,
		hashTable:        *hashTable,
		logCleanerCancel: cancelCleaner,
		mutex:            &mutex,
	}, nil
}

// Set the value of a string key to a string
func (bcs *BitCaskStore) Set(key string, value []byte) (err error) {
	// coarse grained mutex to update hashtable and storage
	bcs.mutex.Lock()
	err = bcs.logStore.Append([]byte(key), value, &bcs.hashTable)
	bcs.mutex.Unlock()
	return err
}

// Get the string value of the a string key. If the key does not exist, return nil.
func (bcs *BitCaskStore) Get(key string) (value []byte, exists bool, err error) {
	if entry, ok := bcs.hashTable[key]; ok {
		value, err = bcs.logStore.ReadKeyDirEntry(entry)
		return value, ok, err
	}
	return nil, false, nil
}

// Remove a given key
func (bcs *BitCaskStore) Remove(key string) error {
	if _, ok := bcs.hashTable[key]; ok {
		bcs.mutex.Lock()
		if err := bcs.logStore.Append([]byte(key), []byte{}, &bcs.hashTable); err != nil {
			return err
		}
		delete(bcs.hashTable, key)
		bcs.mutex.Unlock()
		return nil
	}
	return errDeletingNonExistingKey
}

func (bcs *BitCaskStore) Close() error {
	// release lock
	if err := bcs.lockFile.Close(); err != nil {
		return err
	}
	bcs.logCleanerCancel()
	bcs.logStore.Close()
	return nil
}
