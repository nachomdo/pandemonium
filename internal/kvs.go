package internal

import (
	"errors"
	"fmt"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
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

type KVStoreCommand struct {
	Key     string
	Value   string
	Command Command
}

type KVStore interface {
	io.Closer

	// Set the value of a string key to a string
	Set(key string, value string) error

	// Get the string value of the a string key. If the key does not exist, return nil.
	Get(key string) (*string, error)

	// Remove a given key
	Remove(key string) error
}

type BitCaskStore struct {
	logStore  *logBasedStorage
	basePath  string
	lockFile  *os.File
	hashTable keyDirTable
	mutex     chan struct{}
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

	hashTable, err := logStore.buildKeyDirTable()
	if err != nil {
		return nil, err
	}

	return &BitCaskStore{
		basePath:  path,
		logStore:  logStore,
		lockFile:  lockFile,
		hashTable: *hashTable,
		mutex:     make(chan struct{}, 1),
	}, nil
}

// Set the value of a string key to a string
func (bcs *BitCaskStore) Set(key string, value string) error {
	log.Debugf("setting key %v value %v", key, value)

	// coarse grained mutex to update hashtable and storage
	bcs.mutex <- struct{}{}

	<-bcs.mutex
	return nil
}

// Get the string value of the a string key. If the key does not exist, return nil.
func (bcs *BitCaskStore) Get(key string) *string {
	log.Debugf("getting key %v", key)

	return nil
}

// Remove a given key
func (*BitCaskStore) Remove(key string) error {
	log.Debugf("removing key %v", key)
	return nil
}

func (bcs *BitCaskStore) Close() error {
	// release lock
	if err := bcs.lockFile.Close(); err != nil {
		return err
	}

	// TODO: flush pending changes to disk

	return nil
}
