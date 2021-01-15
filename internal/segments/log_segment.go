package segments

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"golang.org/x/exp/mmap"
	"pingcap.com/kvs/internal/segments/encoding"
)

var (
	regexpSegmentNameFormat = regexp.MustCompile(`segment_(\d{5}).dat`)
	errNoActiveSegment      = errors.New("error rotating non active segment")
)

const (
	MaxSegmentSizeBytes = 1 * 1024 * 1024
)

type LogSegment struct {
	// read path
	ra *mmap.ReaderAt
	r  *os.File
	// write path
	fd      *os.File
	encoder encoding.Serializable
	// other fields
	path          string
	segmentSize   int64
	segmentID     int
	activeSegment bool
}

func NewLogSegment(path string, active bool) (*LogSegment, error) {
	var fd *os.File
	var r *os.File
	var err error
	var ra *mmap.ReaderAt
	if active {
		fd, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			return nil, fmt.Errorf("error opening active segment for writing: %v", err)
		}
		r, err = os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("error opening active segment for reading: %v", err)
		}
	} else {
		ra, err = mmap.Open(path)
		if err != nil {
			return nil, fmt.Errorf("error opening segment file: %v", err)
		}
	}
	return &LogSegment{
		ra:            ra,
		path:          path,
		fd:            fd,
		r:             r,
		activeSegment: active,
		encoder:       encoding.NewBitCaskEncoder(fd),
		segmentID:     SegmentID(path, active),
	}, nil
}

func (ls *LogSegment) ReadAll() (*KeyDirTable, error) {
	var decoder *encoding.BitCaskDecoder

	if ls.activeSegment {
		decoder = encoding.NewBitCaskDecoder(ls.r)
	} else {
		segmentReader := io.NewSectionReader(ls.ra, 0, int64(ls.ra.Len()))
		decoder = encoding.NewBitCaskDecoder(segmentReader)
	}
	var offset int64
	kdir := make(KeyDirTable)
	for {
		key, _, bytesRead, err := decoder.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading segment record: %w", err)
		}
		kdir[string(key)] = NewKeyDirEntry(ls.segmentID, offset, bytesRead)
		offset += bytesRead
	}
	ls.segmentSize = offset
	return &kdir, nil
}

func (ls *LogSegment) ReadAt(offset, n int64) ([]byte, []byte, error) {
	var decoder *encoding.BitCaskDecoder
	if ls.activeSegment {
		buffer := make([]byte, n)
		if _, err := ls.r.ReadAt(buffer, offset); err != nil {
			return nil, nil, err
		}
		decoder = encoding.NewBitCaskDecoder(bytes.NewReader(buffer))
	} else {
		decoder = encoding.NewBitCaskDecoder(io.NewSectionReader(ls.ra, offset, n))
	}
	key, value, _, err := decoder.ReadNext()

	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

func SegmentID(path string, activeSegment bool) int {
	// for the active segment we calculate the next consecutive ID
	// based on the datafiles present on the folder
	if activeSegment {
		basePath := filepath.Dir(path)
		segments, err := filepath.Glob(fmt.Sprintf("%s/segment_*.dat", basePath))
		if err != nil {
			return -1
		}

		// only active segments exists
		if len(segments) == 0 {
			return 1
		}
		lastSegment := segments[len(segments)-1]

		matches := regexpSegmentNameFormat.FindAllStringSubmatch(lastSegment, -1)
		if len(matches) == 0 {
			return 1
		}
		fileID, _ := strconv.Atoi(matches[0][1])
		return fileID + 1
	}
	matches := regexpSegmentNameFormat.FindAllStringSubmatch(path, -1)
	if len(matches) == 0 {
		return -1
	}
	fileID, _ := strconv.Atoi(matches[0][1])

	return fileID
}

func (ls *LogSegment) Write(key, value []byte) (*KeyDirEntry, error) {
	offset := ls.segmentSize
	readBytes, err := ls.encoder.Write(key, value)
	if err != nil {
		return nil, fmt.Errorf("error appending to active segment: %w", err)
	}
	ls.segmentSize += readBytes
	return NewKeyDirEntry(ls.segmentID, offset, readBytes), nil
}

func (ls *LogSegment) Size() int64 {
	return ls.segmentSize
}

func (ls *LogSegment) Rotate() (err error) {

	if !ls.activeSegment {
		return errNoActiveSegment
	}
	newPath := filepath.Join(filepath.Dir(ls.path),
		fmt.Sprintf("segment_%05d.dat", ls.segmentID))

	ls.activeSegment = false

	if err = ls.fd.Sync(); err != nil {
		return err
	}
	if err = ls.fd.Close(); err != nil {
		return err
	}
	if err = ls.r.Close(); err != nil {
		return err
	}
	if err = os.Rename(ls.path, newPath); err != nil {
		return err
	}

	if ls.ra, err = mmap.Open(newPath); err != nil {
		return err
	}

	return nil
}

func (ls *LogSegment) Close() error {
	if ls.activeSegment {
		// FSync to disk before closing
		if err := ls.fd.Sync(); err != nil {
			return fmt.Errorf("error syncing with disk: %w", err)
		}
		if err := ls.fd.Close(); err != nil {
			return fmt.Errorf("error closing append only fd: %w", err)
		}
		if err := ls.r.Close(); err != nil {
			return fmt.Errorf("error closing read only fd: %w", err)
		}
	} else {
		if err := ls.ra.Close(); err != nil {
			return fmt.Errorf("error closing mmap fd: %w", err)
		}
	}
	return nil
}
