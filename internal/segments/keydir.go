package segments

import "sync"

type KeyDirEntry struct {
	FileID int
	Offset int64
	Size   int64
}

type KeyDirTable struct {
	sync.RWMutex
	data map[string]*KeyDirEntry
}

func NewKeyDirTable() KeyDirTable {
	return KeyDirTable{
		data: make(map[string]*KeyDirEntry),
	}
}

func (kdt KeyDirTable) Get(key string) (entry *KeyDirEntry, ok bool) {
	kdt.RLock()
	entry, ok = kdt.data[key]
	kdt.RUnlock()
	return entry, ok
}

func (kdt KeyDirTable) Set(key string, entry *KeyDirEntry) {
	kdt.Lock()
	kdt.data[key] = entry
	kdt.Unlock()
}

func (kdt KeyDirTable) Remove(key string) {
	kdt.Lock()
	delete(kdt.data, key)
	kdt.Unlock()
}

func (kdt KeyDirTable) Len() (l int) {
	kdt.RLock()
	l = len(kdt.data)
	kdt.RUnlock()
	return l
}

type ForEachFn func(key string, value *KeyDirEntry)

func (kdt KeyDirTable) ForEach(fn ForEachFn) {
	kdt.RLock()
	for k, v := range kdt.data {
		fn(k, v)
	}
	kdt.RUnlock()
}

func NewKeyDirEntry(fileID int, offset int64, size int64) *KeyDirEntry {
	return &KeyDirEntry{
		FileID: fileID,
		Offset: offset,
		Size:   size,
	}
}
