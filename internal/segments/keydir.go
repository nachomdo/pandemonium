package segments

type KeyDirEntry struct {
	FileID int
	Offset int64
	Size   int64
}

type KeyDirTable map[string]*KeyDirEntry

func NewKeyDirEntry(fileID int, offset int64, size int64) *KeyDirEntry {
	return &KeyDirEntry{
		FileID: fileID,
		Offset: offset,
		Size:   size,
	}
}
