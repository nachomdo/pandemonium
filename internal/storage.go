package internal

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const (
	ActiveSegmentFilename = "current_segment.dat"
)

type logBasedStorage struct {
	dataFiles   []os.FileInfo
	currentFile *os.File
	threshold   int
}

type keyDirEntry struct {
	fileID string
	offset int
	vsize  int
}

type keyDirTable map[string]keyDirEntry

func NewLogBasedStorage(path string) (*logBasedStorage, error) {
	var dataFiles []os.FileInfo
	var currentFile *os.File

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("error opening keydir folder: %v", err)
	}
	for _, f := range files {
		if !f.IsDir() {
			// already exists a previously active segment
			if strings.Contains("current", f.Name()) {
				currentFile, err = os.OpenFile(f.Name(), os.O_APPEND, os.ModeAppend)
				if err != nil {
					return nil, fmt.Errorf("error opening active segment: %v", err)
				}
			}
			dataFiles = append(dataFiles, f)
		}
	}

	// No active segment exists
	if len(dataFiles) == 0 {
		currentFile, err = os.OpenFile(ActiveSegmentFilename, os.O_CREATE|os.O_APPEND, os.ModeAppend)
		if err != nil {
			return nil, fmt.Errorf("error opening active segment: %v", err)
		}
	}
	return &logBasedStorage{
		dataFiles:   dataFiles,
		currentFile: currentFile,
	}, nil
}

func readSegment(segment io.ReadSeeker, filePath string) *keyDirTable {
	kdt := make(keyDirTable)
	var readItems int

	bufReader := bufio.NewReader(segment)
	gobDecoder := gob.NewDecoder(bufReader)
	previous := 0
	for {
		var data KVStoreCommand
		current, _ := segment.Seek(0, io.SeekCurrent)
		buffered := bufReader.Buffered()
		fmt.Printf("current %v previous %v buffered %v \n", current, previous, bufReader.Buffered())
		if err := gobDecoder.Decode(&data); err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalf("error reading segment: %v", err)
			}
		}
		switch data.Command {
		case SetKey:
			kdt[data.Key] = keyDirEntry{
				fileID: filePath,
				offset: int(current) - buffered,
			}
			break
		case RemoveKey:
			delete(kdt, data.Key)
			break
		}
		previous = int(current)
		readItems++
	}
	return &kdt
}

func (lbs *logBasedStorage) buildKeyDirTable() (*keyDirTable, error) {
	return nil, nil
}
