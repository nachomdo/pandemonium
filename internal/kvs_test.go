package internal

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenStore(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)
	db, err := OpenBitCaskStore(path)
	assert.NoError(t, err)

	assert.NotNil(t, db, "expected db handler but found nil")

	_, err = OpenBitCaskStore(path)

	assert.Error(t, err, "expected error due to locking but found none")
}

func TestGetNonExistentKey(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)
	db, err := OpenBitCaskStore(path)
	assert.NoError(t, err)
	_, ok, err := db.Get("noname")
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestRemoveNonExistentKey(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)

	db, err := OpenBitCaskStore(path)
	assert.NoError(t, err)

	assert.Error(t, db.Remove("noname"), errDeletingNonExistingKey)
}

func TestGetStoredKey(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)

	db, err := OpenBitCaskStore(path)
	assert.NoError(t, err)
	assert.NoError(t, db.Set("entry", []byte("exit")))
	value, ok, err := db.Get("entry")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "exit", string(value))
}

func TestRemoveStoredKey(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)

	db, err := OpenBitCaskStore(path)
	assert.NoError(t, err)

	assert.NoError(t, db.Set("1", []byte("walnuts")))
	assert.NoError(t, db.Set("2", []byte("peanuts")))
	assert.NoError(t, db.Set("3", []byte("peas")))
	assert.NoError(t, db.Remove("2"))

	_, ok, err := db.Get("2")
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestOverwriteExistingKey(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)

	db, err := OpenBitCaskStore(path)
	assert.NoError(t, err)

	assert.NoError(t, db.Set("1", []byte("walnuts")))
	assert.NoError(t, db.Set("2", []byte("peanuts")))
	assert.NoError(t, db.Set("3", []byte("peas")))
	assert.NoError(t, db.Set("2", []byte("brocoli")))

	value, ok, err := db.Get("2")
	assert.True(t, ok)
	assert.Equal(t, "brocoli", string(value))
}

func BenchmarkWriting(b *testing.B) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)
	db, err := OpenBitCaskStore(path)
	assert.NoError(b, err)

	value := bytes.Repeat([]byte{0xa}, 4096)
	b.ResetTimer()
	b.SetBytes(4096)
	for i := 0; i < b.N; i++ {
		db.Set(strconv.Itoa(i), value)
	}
}

func BenchmarkReading(b *testing.B) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")
	defer os.RemoveAll(path)

	db, err := OpenBitCaskStore(path)
	assert.NoError(b, err)
	value := bytes.Repeat([]byte{0xa}, 4096)
	key := "b12"

	assert.NoError(b, db.Set(key, value))
	b.ResetTimer()

	b.SetBytes(4096)
	for i := 0; i < b.N; i++ {
		db.Get("b12")
	}
}
