package encoding

import (
	"bytes"
	"os"
	"syscall"
	"unsafe"
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
	flags := syscall.MAP_PRIVATE | syscall.MAP_POPULATE
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, flags)
	madvise(data, syscall.MADV_SEQUENTIAL|syscall.MADV_WILLNEED)
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

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
