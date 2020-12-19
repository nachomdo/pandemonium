package internal

import (
	log "github.com/sirupsen/logrus"
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
	// Set the value of a string key to a string
	Set(key string, value string) error

	// Get the string value of the a string key. If the key does not exist, return nil.
	Get(key string) (*string, error)

	// Remove a given key
	Remove(key string) error
}

type BitCaskStore struct{}

// Set the value of a string key to a string
func (*BitCaskStore) Set(key string, value string) error {
	log.Debugf("setting key %v value %v", key, value)

	return nil
}

// Get the string value of the a string key. If the key does not exist, return nil.
func (*BitCaskStore) Get(key string) *string {
	log.Debugf("getting key %v", key)
	return nil
}

// Remove a given key
func (*BitCaskStore) Remove(key string) error {
	log.Debugf("removing key %v", key)
	return nil
}
