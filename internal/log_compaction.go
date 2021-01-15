package internal

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"pingcap.com/kvs/internal/segments"
)

const (
	CleanNonUsed Policy = iota
	CleanDirtyRatio
)
const (
	cleaningInterval = 10 * time.Second
)

type Policy byte

type LogCleaner interface {
	Clean(*context.Context)
}

type simpleLogCleaner struct {
	basePath string
	kdt      *segments.KeyDirTable
	mutex    *sync.RWMutex
}

func NewLogCleanerWithPolicy(basePath string, mutex *sync.RWMutex, kdt *segments.KeyDirTable, cleanPolicy Policy) LogCleaner {
	switch cleanPolicy {
	case CleanNonUsed:
		return &simpleLogCleaner{
			basePath: basePath,
			kdt:      kdt,
			mutex:    mutex,
		}
	default:
		return nil
	}

}

func (slc *simpleLogCleaner) Clean(ctx *context.Context) {
	go func() {
		timerCh := time.Tick(cleaningInterval)
		for range timerCh {
			select {
			case <-timerCh:
				slc.cleanUnusedFiles()
			case <-(*ctx).Done():
				logrus.Info("exiting logCleaner background goroutine")
				return
			}
		}
	}()
}

func (slc *simpleLogCleaner) cleanUnusedFiles() {
	files, _ := filepath.Glob(filepath.Join(slc.basePath, "segment_*.dat"))

	// :troll: :troll:
	bloomFilter := make(map[int]bool, len(files))
	slc.mutex.RLock()
	for _, v := range *slc.kdt {
		bloomFilter[v.FileID] = true
	}
	slc.mutex.RUnlock()
	for _, f := range files {
		if _, ok := bloomFilter[segments.SegmentID(f, false)]; !ok {
			os.Remove(f)
		}
	}
}
