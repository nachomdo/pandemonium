package encoding

import (
	"bytes"
	"os"
	"syscall"
)

func NewBitCaskMmapDecoder(filePath string) *BitCaskMmapDecoder {
	f, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil
	}

	size := fi.Size()
	flags := syscall.MAP_PRIVATE
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, flags)
	if err != nil {
		return nil
	}
	return &BitCaskMmapDecoder{
		data: data,
		BitCaskDecoder: BitCaskDecoder{
			r: bytes.NewReader(data),
		},
	}
}
