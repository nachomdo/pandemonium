package internal

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"pingcap.com/kvs/internal/segments"
	"pingcap.com/kvs/internal/segments/encoding"
)

func emptyDataFolder(t *testing.T) string {
	basePath, err := ioutil.TempDir("/tmp", "bitcask*")
	assert.NoError(t, err)
	return basePath
}

func existingDataFolderWithSegments(t *testing.T, segments int) string {
	basePath, err := ioutil.TempDir("/tmp", "bitcask*")
	assert.NoError(t, err)

	for i := 1; i <= segments; i++ {
		fd, err := os.Create(fmt.Sprintf("%s/segment_%05d.dat", basePath, i))
		assert.NoError(t, err)
		j := i * 10
		max := j + 10
		encoder := encoding.NewBitCaskEncoder(fd)
		for ; j < max; j++ {
			_, err := encoder.Write([]byte(strconv.Itoa(j)), []byte(fmt.Sprintf("value for key %d", j)))
			assert.NoError(t, err)
		}

		fd.Close()
	}
	return basePath
}

func TestLogBasedStorage(t *testing.T) {
	path := existingDataFolderWithSegments(t, 5)
	defer os.RemoveAll(path)

	lbs, err := NewLogBasedStorage(path)
	assert.NoError(t, err)
	kdt, err := lbs.BuildKeyDirTable()
	assert.NoError(t, err)
	assert.Equal(t, 50, len(*kdt))
	t.Run("read the whole key dir structure", func(t *testing.T) {
		for k, v := range *kdt {
			rv, err := lbs.ReadKeyDirEntry(v)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("value for key %s", k), string(rv))
		}
	})

	t.Run("append new data", func(t *testing.T) {
		err := lbs.Append([]byte("9999"), []byte("value for key 9999"), kdt)
		assert.NoError(t, err)
		lastEntry := (*kdt)["9999"]
		lastValue, err := lbs.ReadKeyDirEntry(lastEntry)
		assert.NoError(t, err)
		assert.Equal(t, "value for key 9999", string(lastValue))
	})

	t.Run("close all resources", func(t *testing.T) {
		assert.NoError(t, lbs.Close())
		// storage closed cannot read values
		entry := (*kdt)["10"]
		_, err := lbs.ReadKeyDirEntry(entry)
		assert.Error(t, err, "mmap: Closed")
	})
}
func TestSegmentsRotation(t *testing.T) {
	basePath := emptyDataFolder(t)
	defer os.RemoveAll(basePath)
	lbs, err := NewLogBasedStorage(basePath)
	assert.NoError(t, err)
	k := []byte("mykey")
	v := bytes.Repeat([]byte{0xb}, 1024)
	kdt := make(segments.KeyDirTable)
	for i := 0; i < 118000; i++ {
		assert.NoError(t, lbs.Append(k, v, &kdt))
	}
	assert.True(t, len(lbs.dataFiles) > 1)
}
