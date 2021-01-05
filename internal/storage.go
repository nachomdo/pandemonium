package internal

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"

	"golang.org/x/exp/mmap"
)

const (
	activeSegmentFilename = "current_segment.dat"
	segmentFilenameFmt    = "segment_%5d.dat"
)

type logSegment struct {
	fd      *os.File
	reader  *mmap.ReaderAt
	current bool
	decoder *gob.Decoder
	encoder *gob.Encoder
}

func NewLogSegment(basePath string) (*logSegment, error) {
	var fd *os.File
	var reader *mmap.ReaderAt
	var current bool
	var err error

	// already exists a previously active segment
	if strings.Contains("current", basePath) {
		fd, err = os.OpenFile(basePath, os.O_APPEND, os.ModeAppend)
		if err != nil {
			return nil, fmt.Errorf("error opening active segment: %v", err)
		}
		reader, err = mmap.Open(basePath)
		if err != nil {
			return nil, fmt.Errorf("error opening active segment: %v", err)
		}
		current = true
	} else {
		reader, err = mmap.Open(basePath)
		if err != nil {
			return nil, fmt.Errorf("error opening segment file: %v", err)
		}
	}
	encoder := gob.NewEncoder(fd)

	decoder := gob.NewDecoder(fd)

	return &logSegment{
		fd:      fd,
		reader:  reader,
		decoder: decoder,
		encoder: encoder,
		current: current,
	}, nil
}

func (ls *logSegment) appendEntry(cmd KVStoreCommand) (*keyDirEntry, error) {
	if err := ls.encoder.Encode(cmd); err != nil {
		return nil, fmt.Errorf("error appending command to the log: %v", err)
	}

	//ftell to get last position
	offset, err := ls.fd.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("error retrieving offset from appended entry: %v", err)
	}
	return &keyDirEntry{
		offset: offset,
	}, nil
}

type logBasedStorage struct {
	dataFiles   map[string]*logSegment
	currentFile *os.File
	threshold   int
}

type keyDirEntry struct {
	fileID string
	offset int64
	vsize  int
}

type keyDirTable map[string]keyDirEntry

func NewLogBasedStorage(path string) (*logBasedStorage, error) {
	var dataFiles map[string]*logSegment
	var currentFile *os.File

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("error opening keydir folder: %v", err)
	}
	for _, f := range files {
		if !f.IsDir() && !strings.Contains(f.Name(), lockFilename) {
			segment, err := NewLogSegment(path)
			if err != nil {
				return nil, fmt.Errorf("error creating log segment for %s: %v", path, err)
			}
			dataFiles[path] = segment
		}
	}

	// No active segment exists
	if len(dataFiles) == 0 {
		currentFile, err = os.OpenFile(fmt.Sprintf("%s/%s", path, activeSegmentFilename), os.O_CREATE|os.O_APPEND, os.ModeAppend)
		if err != nil {
			return nil, fmt.Errorf("error opening active segment: %v", err)
		}
	}
	return &logBasedStorage{
		dataFiles:   dataFiles,
		currentFile: currentFile,
	}, nil
}

func readSegment(segment *mmap.ReaderAt, filePath string) *keyDirTable {
	kdt := make(keyDirTable)
	var readItems int
	buffer := bytes.NewBuffer([]byte{})
	bufReader := bufio.NewReader(io.TeeReader(io.NewSectionReader(segment, 0, int64(segment.Len())), buffer))
	gobDecoder := gob.NewDecoder(bufReader)
	previous := 0
	offset := 0
	for {
		var data KVStoreCommand
		current := previous
		buffered := bufReader.Buffered()
		fmt.Printf("current %v previous %v buffered %v \n", current, previous, buffered)
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
				offset: int64(offset),
			}
			break
		case RemoveKey:
			delete(kdt, data.Key)
			break
		}
		previous = buffer.Len() - bufReader.Buffered()
		offset += previous
		fmt.Printf("** current %v offset %v buffered %v \n", buffer.Len(), offset, bufReader.Buffered())
		buffer.Reset()
		bufReader.Reset(io.TeeReader(io.NewSectionReader(segment, int64(offset), int64(segment.Len()-previous)), buffer))
		readItems++
	}
	return &kdt
}

func mergeTables(kdtSrc, kdtTgt keyDirTable) *keyDirTable {
	for k, v := range kdtSrc {
		kdtTgt[k] = v
	}
	return &kdtTgt
}

func (lbs *logBasedStorage) buildKeyDirTable() (*keyDirTable, error) {

	keys := make([]string, 0, len(lbs.dataFiles))
	for k := range lbs.dataFiles {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var kdt keyDirTable
	for _, k := range keys {
		kdtTmp := readSegment(lbs.dataFiles[k].reader, k)
		kdt = *mergeTables(*kdtTmp, kdt)
	}

	return &kdt, nil
}

// func (lbs *logBasedStorage) valueForKeyDirEntry(kde *keyDirEntry) []byte {
// 	fd := lbs.dataFiles[kde.fileID]
// 	fd.Seek(int64(kde.offset), io.SeekStart)
// 	var data KVStoreCommand

// 	fd.Seek(0, io.SeekStart)
// 	myreader := bufio.NewReader(fd)
// 	gobDecoder := gob.NewDecoder(myreader)
// 	gobDecoder.Decode(nil)

// 	fd.Seek(int64(kde.offset), io.SeekStart)
// 	myreader.Reset(fd)
// 	if err := gobDecoder.Decode(&data); err != nil {
// 		return nil
// 	}

// 	return []byte(data.Value)
// }
