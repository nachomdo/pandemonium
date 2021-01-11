package encoding

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundtrip(t *testing.T) {
	data := []struct {
		key   []byte
		value []byte
	}{
		{
			key:   []byte("1"),
			value: []byte("geisha"),
		},
		{
			key:   []byte("2"),
			value: []byte("bourbon"),
		},
		{
			key:   []byte("3"),
			value: []byte("arabica"),
		},
	}

	memBuffer := bytes.NewBuffer([]byte{})
	encoder := NewBitCaskEncoder(memBuffer)

	for _, item := range data {
		written, err := encoder.Write(item.key, item.value)
		assert.NoError(t, err)
		assert.Equal(t, int64(headerSize+len(item.key)+len(item.value)), written)
	}

	t.Logf("written %s", memBuffer.String())
	decoder := NewBitCaskDecoder(memBuffer)
	for _, item := range data {
		key, value, bytesRead, err := decoder.ReadNext()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		assert.Equal(t, int64(headerSize+len(key)+len(value)), bytesRead)
		assert.Equal(t, item.key, key)
		assert.Equal(t, item.value, value)
		t.Logf("reading key %s value %s", key, value)
	}
}
