package internal

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenStore(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")

	db, err := OpenBitCaskStore(path)

	if err != nil {
		t.Error(err)
	}

	assert.NotNil(t, db, "expected db handler but found nil")

	_, err = OpenBitCaskStore(path)

	assert.Error(t, err, "expected error due to locking but found none")
}

func TestGetNonExistentKey(t *testing.T) {

}

func TestRemoveNonExistentKey(t *testing.T) {

}

func TestGetStoredKey(t *testing.T) {

}

func TestRemoveStoredKey(t *testing.T) {

}

func TestSetNonExistentKey(t *testing.T) {
	path, _ := ioutil.TempDir("/tmp", "kvstore_*")

	db, _ := OpenBitCaskStore(path)

	assert.Nil(t, db.Set("1", "Pophams Bakery"), "expected no error setting key")
	assert.Equal(t, *db.Get("1"), "Pophams Bakery")

}

func TestOverwriteExistingKey(t *testing.T) {

}
