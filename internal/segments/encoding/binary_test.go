package encoding

import (
	"bytes"
	"io"
	"io/ioutil"
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
func TestMMapRoundtrip(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "roundtrip_*.dat")
	assert.NoError(t, err)

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

	encoder := NewBitCaskEncoder(f)
	type offsetSize struct {
		offset int64
		size   int64
	}
	recordsLocation := make([]offsetSize, len(data))
	var offset int64
	for i, item := range data {
		written, err := encoder.Write(item.key, item.value)
		recordsLocation[i] = offsetSize{offset, written}
		offset += written
		assert.NoError(t, err)
		assert.Equal(t, int64(headerSize+len(item.key)+len(item.value)), written)
	}
	f.Sync()
	f.Close()
	decoder := NewBitCaskMmapDecoder(f.Name())
	for i, item := range data {
		loc := recordsLocation[i]
		key, value, err := decoder.ReadAt(loc.offset, loc.size)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		assert.Equal(t, item.key, key)
		assert.Equal(t, item.value, value)
		t.Logf("reading key %s value %s", key, value)
	}
}
