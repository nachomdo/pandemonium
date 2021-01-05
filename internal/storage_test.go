package internal

import (
	"bufio"
	"encoding/gob"
	"io"
	"io/ioutil"
	"log"
	"testing"

	"golang.org/x/exp/mmap"
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

	fd, err := ioutil.TempFile("/tmp", activeSegmentFilename)

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

func TestLogWriter(t *testing.T) {

}

func TestLogReader(t *testing.T) {

}

func TestLoadKeydirStructure(t *testing.T) {
	path := generateActiveSegment()
	var fd *mmap.ReaderAt
	var err error
	if fd, err = mmap.Open(path); err != nil {
		t.Fatal(err)
	}

	defer fd.Close()

	kdt := *readSegment(fd, path)
	t.Log(kdt)
	entry, _ := kdt["3"]

	var data KVStoreCommand

	myreader := bufio.NewReader(io.NewSectionReader(fd, 0, int64(fd.Len())))
	gobDecoder := gob.NewDecoder(myreader)
	gobDecoder.Decode(nil)

	myreader.Reset(io.NewSectionReader(fd, entry.offset, int64(fd.Len()-int(entry.offset))))
	if err := gobDecoder.Decode(&data); err != nil {
		t.Fatal(err)
	}
	t.Log(data.Value)

}
