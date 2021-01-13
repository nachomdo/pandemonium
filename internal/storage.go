package internal

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"

	"pingcap.com/kvs/internal/segments"
)

const (
	activeSegmentFilename = "current_segment.dat"
	segmentFilenameFmt    = "segment_%05d.dat"
)

type LogStorage interface {
	BuildKeyDirTable() (*segments.KeyDirTable, error)
	ReadKeyDirEntry(entry *segments.KeyDirEntry) ([]byte, error)
	Append(key []byte, value []byte, kdt *segments.KeyDirTable) error
	Close() error
}

type logBasedStorage struct {
	dataFiles      map[int]*segments.LogSegment
	currentSegment *segments.LogSegment
	basePath       string
	threshold      int
}

func NewLogBasedStorage(path string) (*logBasedStorage, error) {
	var currentSegment *segments.LogSegment

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("error opening keydir folder: %v", err)
	}

	var active bool
	dataFiles := make(map[int]*segments.LogSegment, len(files))
	for _, f := range files {
		if !f.IsDir() && !strings.Contains(f.Name(), lockFilename) {
			if f.Name() == activeSegmentFilename {
				active = true
			}
			fullPath := filepath.Join(path, f.Name())
			segment, err := segments.NewLogSegment(fullPath, active)
			if err != nil {
				return nil, fmt.Errorf("error creating log segment for %s: %v", path, err)
			}
			segmentID := segments.SegmentID(fullPath, active)
			dataFiles[segmentID] = segment
		}
	}

	// No active segment exists
	if len(dataFiles) == 0 || !active {
		fullPath := filepath.Join(path, activeSegmentFilename)
		currentSegment, err = segments.NewLogSegment(fullPath, true)
		if err != nil {
			return nil, fmt.Errorf("error opening active segment: %v", err)
		}
	}
	return &logBasedStorage{
		dataFiles:      dataFiles,
		currentSegment: currentSegment,
		basePath:       path,
	}, nil
}

func mergeTables(kdtSrc, kdtTgt segments.KeyDirTable) *segments.KeyDirTable {
	if kdtSrc == nil {
		return &kdtTgt
	}
	if kdtTgt == nil {
		return &kdtSrc
	}
	for k, v := range kdtSrc {
		kdtTgt[k] = v
	}
	return &kdtTgt
}

func (lbs *logBasedStorage) BuildKeyDirTable() (*segments.KeyDirTable, error) {

	keys := make([]int, 0, len(lbs.dataFiles))
	for k := range lbs.dataFiles {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	kdt := make(segments.KeyDirTable)
	for _, k := range keys {
		kdtTmp, err := lbs.dataFiles[k].ReadAll()
		if err != nil {
			return nil, fmt.Errorf("error building key dir table: %w", err)
		}
		kdt = *mergeTables(*kdtTmp, kdt)
	}

	return &kdt, nil
}

func (lbs *logBasedStorage) ReadKeyDirEntry(entry *segments.KeyDirEntry) (value []byte, err error) {
	if segment, ok := lbs.dataFiles[entry.FileID]; ok {
		_, value, err = segment.ReadAt(entry.Offset, entry.Size)
	} else {
		_, value, err = lbs.currentSegment.ReadAt(entry.Offset, entry.Size)
	}

	return value, err
}

func (lbs *logBasedStorage) Append(key []byte, value []byte, kdt *segments.KeyDirTable) error {
	if lbs.currentSegment.Size() > segments.MaxSegmentSizeBytes {
		if err := lbs.rotateSegments(); err != nil {
			return err
		}
	}
	kde, err := lbs.currentSegment.Write(key, value)
	if err != nil {
		return err
	}
	(*kdt)[string(key)] = kde
	return nil
}

func (lbs *logBasedStorage) rotateSegments() (err error) {
	fullPath := filepath.Join(lbs.basePath, activeSegmentFilename)
	if err := lbs.currentSegment.Rotate(); err != nil {
		return err
	}

	segmentID := len(lbs.dataFiles) + 1
	lbs.dataFiles[segmentID] = lbs.currentSegment
	lbs.currentSegment, err = segments.NewLogSegment(fullPath, true)
	return err
}

func (lbs *logBasedStorage) Close() error {
	for _, segment := range lbs.dataFiles {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	if err := lbs.currentSegment.Close(); err != nil {
		return err
	}
	return nil
}
