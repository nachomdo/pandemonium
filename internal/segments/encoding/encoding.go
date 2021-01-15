package encoding

import (
	"errors"
)

var (
	errInvalidMagicNumber = errors.New("error due to unexpected record magic number")
	errSerializingData    = errors.New("error serializing data to underlying medium")
)

type Serializable interface {
	Write(key, value []byte) (int64, error)
}

type Deserializable interface {
	ReadNext() ([]byte, []byte, int64, error)
}

type Serde interface {
	Serializable
	Deserializable
}
