package encoding

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	magicSize   = 4
	keySize     = 4
	valueSize   = 8
	magicNumber = 0xc0ff33
	headerSize  = magicSize + keySize + valueSize
)

type BitCaskEncoder struct {
	w *bufio.Writer
}

type BitCaskDecoder struct {
	r io.Reader
}

type BitCaskMmapDecoder struct {
	BitCaskDecoder
	data []byte
}

func NewBitCaskEncoder(w io.Writer) *BitCaskEncoder {
	return &BitCaskEncoder{
		w: bufio.NewWriterSize(w, 1024*1024),
	}
}

func NewBitCaskDecoder(r io.Reader) *BitCaskDecoder {
	return &BitCaskDecoder{
		r: r,
	}
}

func (bce *BitCaskEncoder) Write(key, value []byte) (int64, error) {
	var written int
	buffer := make([]byte, headerSize)

	// magic number
	binary.BigEndian.PutUint32(buffer, uint32(magicNumber))
	// key size
	binary.BigEndian.PutUint32(buffer[magicSize:], uint32(len(key)))
	// value size
	binary.BigEndian.PutUint64(buffer[magicSize+keySize:], uint64(len(value)))

	// dump header to underlying writer
	tmp, err := bce.w.Write(buffer)
	if err != nil {
		return -1, fmt.Errorf("error serialising header: %w", err)
	}
	written += tmp
	tmp, err = bce.w.Write(key)
	if err != nil {
		return -1, fmt.Errorf("error serialising key: %w", err)
	}
	written += tmp

	tmp, err = bce.w.Write(value)
	if err != nil {
		return -1, fmt.Errorf("error serialising key: %w", err)
	}
	written += tmp
	if err := bce.w.Flush(); err != nil {
		return -1, err
	}
	return int64(written), nil
}

func (bce *BitCaskDecoder) ReadNext() ([]byte, []byte, int64, error) {
	headerBuffer := make([]byte, headerSize)
	if _, err := io.ReadFull(bce.r, headerBuffer); err != nil {
		return nil, nil, -1, err
	}
	recordMagicNumber := binary.BigEndian.Uint32(headerBuffer[:magicSize])
	if recordMagicNumber != uint32(magicNumber) {
		return nil, nil, -1, errInvalidMagicNumber
	}

	recordKeyLen := binary.BigEndian.Uint32(headerBuffer[magicSize : magicSize+keySize])
	recordValueLen := binary.BigEndian.Uint64(headerBuffer[magicSize+keySize:])
	keyValueBuffer := make([]byte, uint64(recordKeyLen)+recordValueLen)
	if _, err := io.ReadFull(bce.r, keyValueBuffer); err != nil {
		return nil, nil, -1, err
	}
	bytesRead := int64(len(headerBuffer) + len(keyValueBuffer))
	return keyValueBuffer[:recordKeyLen], keyValueBuffer[recordKeyLen:], bytesRead, nil
}

func (bcd *BitCaskMmapDecoder) ReadAt(offset int64, size int64) ([]byte, []byte, error) {
	buffer := make([]byte, size)
	reader := bytes.NewReader(bcd.data)
	reader.ReadAt(buffer, offset)
	recordMagicNumber := binary.BigEndian.Uint32(buffer[:magicSize])
	if recordMagicNumber != uint32(magicNumber) {
		return nil, nil, errInvalidMagicNumber
	}
	recordKeyLen := binary.BigEndian.Uint32(buffer[magicSize : magicSize+keySize])
	recordValueLen := binary.BigEndian.Uint64(buffer[magicSize+keySize:])
	valueBaseOffset := headerSize + recordKeyLen
	return buffer[headerSize : headerSize+recordKeyLen], buffer[valueBaseOffset : valueBaseOffset+uint32(recordValueLen)], nil

}
