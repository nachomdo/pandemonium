package segments

import (
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	CleanNonUsed Policy = iota
	CleanDirtyRatio
)
const (
	cleaningInterval = 1 * time.Second
)

type Policy byte

type LogCleaner interface {
	Clean() chan struct{}
}

type simpleLogCleaner struct {
	basePath string
	kdt      *KeyDirTable
}

func NewLogCleanerWithPolicy(basePath string, kdt *KeyDirTable, cleanPolicy Policy) LogCleaner {
	switch cleanPolicy {
	case CleanNonUsed:
		return &simpleLogCleaner{
			basePath: basePath,
			kdt:      kdt,
		}
	default:
		return nil
	}

}

func (slc *simpleLogCleaner) Clean() chan struct{} {
	controlCh := make(chan struct{})
	go func(ctlCh chan struct{}) {
		for {
			time.Sleep(cleaningInterval)
			select {
			case _, closed := <-ctlCh:
				if closed {
					logrus.Info("exiting logCleaner background goroutine")
					return
				}
			default:
				break
			}
			logrus.Info("logCleaner pass")
			cleanUnusedFiles(slc.basePath, slc.kdt)
		}
	}(controlCh)
	return controlCh
}

func cleanUnusedFiles(basePath string, kdt *KeyDirTable) {
	files, _ := filepath.Glob(filepath.Join(basePath, "segment_*.dat"))

	// :troll: :troll:
	bloomFilter := make(map[int]bool, len(files))
	kdt.RLock()
	for _, v := range kdt.Data {
		bloomFilter[v.FileID] = true
	}
	kdt.RUnlock()
	for _, f := range files {
		if _, ok := bloomFilter[SegmentID(f, false)]; !ok {
			os.Remove(f)
		}
	}
}
