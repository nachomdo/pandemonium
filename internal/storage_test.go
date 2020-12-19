package internal

import (
	"bufio"
	"encoding/gob"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func generateActiveSegment() string {

	commands := []KVStoreCommand{
		KVStoreCommand{
			Key:     "1",
			Value:   "Rosslyn",
			Command: SetKey,
		},
		KVStoreCommand{
			Key:     "2",
			Value:   "Pophams",
			Command: SetKey,
		},
		KVStoreCommand{
			Key:     "3",
			Value:   "Gentleman Baristas",
			Command: SetKey,
		},
		KVStoreCommand{
			Key:     "2",
			Value:   "Pophams Bakery",
			Command: SetKey,
		},
		KVStoreCommand{
			Key:     "4",
			Value:   "Starbucks Coffee",
			Command: SetKey,
		},
		KVStoreCommand{
			Key:     "4",
			Command: RemoveKey,
		},
	}

	fd, err := ioutil.TempFile("/tmp", ActiveSegmentFilename)

	defer fd.Close()

	if err != nil {
		log.Fatalf("cannot generate active segment: %v", err)
	}
	gobEncoder := gob.NewEncoder(fd)
	for _, cmd := range commands {

		if err := gobEncoder.Encode(cmd); err != nil {
			log.Fatalf("cannot write to active segment: %v", err)
		}
	}

	return fd.Name()
}

func TestLoadKeydirStructure(t *testing.T) {
	path := generateActiveSegment()
	var fd *os.File
	var err error
	if fd, err = os.Open(path); err != nil {
		t.Fatal(err)
	}

	defer fd.Close()

	kdt := *readSegment(fd, path)
	t.Log(kdt)
	entry, _ := kdt["3"]

	var data KVStoreCommand

	fd.Seek(0, io.SeekStart)

	x, _ := fd.Seek(int64(0), io.SeekStart)

	myreader := bufio.NewReader(fd)
	gobDecoder := gob.NewDecoder(myreader)
	gobDecoder.Decode(nil)

	x, _ = fd.Seek(int64(entry.offset), io.SeekStart)
	myreader.Reset(fd)
	if err := gobDecoder.Decode(&data); err != nil {
		t.Fatal(err)
	}
	t.Log(data.Value, x)

}
