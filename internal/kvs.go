package internal

import (
	"errors"
	"fmt"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
	"pingcap.com/kvs/internal/segments"
)

const (
	lockFilename = ".locked"
)

type Command int

const (
	GetKey Command = iota
	SetKey
	RemoveKey
)

var (
	errInvalidKey             = errors.New("error due to invalid key")
	errDeletingNonExistingKey = errors.New("error removing a key not present in the database")
)

type KVStoreCommand struct {
	Key     string
	Value   string
	Command Command
}

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
	logStore     LogStorage
	basePath     string
	lockFile     *os.File
	hashTable    *segments.KeyDirTable
	logCleanerCh chan struct{}
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
	//logCleaner := segments.NewLogCleanerWithPolicy(path, hashTable, segments.CleanNonUsed)
	//logCleanerCh := logCleaner.Clean()
	logCleanerCh := make(chan struct{})

	return &BitCaskStore{
		basePath:     path,
		logStore:     logStore,
		lockFile:     lockFile,
		hashTable:    hashTable,
		logCleanerCh: logCleanerCh,
	}, nil
}

// Set the value of a string key to a string
func (bcs *BitCaskStore) Set(key string, value []byte) error {
	log.Debugf("setting key %v value %v", key, value)

	// coarse grained mutex to update hashtable and storage
	bcs.hashTable.Lock()
	defer bcs.hashTable.Unlock()

	return bcs.logStore.Append([]byte(key), value, bcs.hashTable)
}

// Get the string value of the a string key. If the key does not exist, return nil.
func (bcs *BitCaskStore) Get(key string) (value []byte, exists bool, err error) {
	log.Debugf("getting key %v", key)
	bcs.hashTable.RLock()
	defer bcs.hashTable.RUnlock()
	if entry, ok := bcs.hashTable.Data[key]; ok {
		value, err = bcs.logStore.ReadKeyDirEntry(entry)
		return value, ok, err
	}
	return nil, false, nil
}

// Remove a given key
func (bcs *BitCaskStore) Remove(key string) error {
	log.Debugf("removing key %v", key)
	if _, ok := bcs.hashTable.Data[key]; ok {
		bcs.hashTable.Lock()
		defer bcs.hashTable.Unlock()
		if err := bcs.logStore.Append([]byte(key), []byte{}, bcs.hashTable); err != nil {
			return err
		}
		delete(bcs.hashTable.Data, key)
		return nil
	}
	return errDeletingNonExistingKey
}

func (bcs *BitCaskStore) Close() error {
	// release lock
	if err := bcs.lockFile.Close(); err != nil {
		return err
	}
	close(bcs.logCleanerCh)
	bcs.logStore.Close()
	return nil
}
