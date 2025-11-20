package kv

import (
	"encoding/binary"
	"fmt"
	"kvstore/skiplist"
	"kvstore/wal"
	"os"
	"sync"
)

type Store struct {
	mem       *skiplist.SkipList
	log       *wal.WAL
	avgAccess float64
	alpha     float64
	mu        sync.RWMutex
	walGen    int64
}

func Open(path string) (*Store, error) {
	// 1. Open WAL
	w, err := wal.Open(path)
	if err != nil {
		return nil, err
	}

	// 2. Inititalize memtable (SkipList)
	mem := skiplist.NewSkipList(0.5, 16)

	// 3. Replay WAL history into the memtable
	err = w.Iterate(func(e wal.Entry) error {
		mem.Put(string(e.Key), e.Value)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Store{
		mem:       mem,
		log:       w,
		avgAccess: 0,
		alpha:     0.4,
	}, nil
}

func (s *Store) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.log.Write(wal.Entry{
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		return err
	}

	s.mem.Put(key, value)
	return nil
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mem.Get(key)
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A. Calculate instantaneous average access count
	var sum float64
	var count float64

	s.mem.Iterator(func(key string, value []byte, accessCount int64) bool {
		sum += float64(accessCount)
		count++
		return true
	})

	var currentMean float64
	if count > 0 {
		currentMean = sum / count
	} else {
		currentMean = 0
	}

	s.avgAccess = (s.alpha * currentMean) + ((1 - s.alpha) * s.avgAccess)

	// 1. Create new MemTable + WAL
	newMem := skiplist.NewSkipList(0.5, 16)

	s.walGen++

	walFilename := fmt.Sprintf("wal-%d.log", s.walGen)

	newWal, err := wal.Open(walFilename)
	if err != nil {
		return err
	}

	cleanupNewWal := func() {
		_ = newWal.Close()
	}

	// 2. Open SSTable file for cold nodes
	sstFile, err := os.OpenFile("store.sst", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer sstFile.Close()

	// 3. Iterate through OLD memtable and migrate
	err = func() error {
		var iterErr error
		s.mem.Iterator(func(key string, value []byte, accessCount int64) bool {
			if float64(accessCount) > s.avgAccess {
				newMem.Put(key, value)
				if werr := newWal.Write(wal.Entry{Key: []byte(key), Value: value}); werr != nil {
					iterErr = werr
					return false
				}
			} else {
				header := make([]byte, 16)
				binary.LittleEndian.PutUint64(header[0:8], uint64(len(key)))
				binary.LittleEndian.PutUint64(header[8:16], uint64(len(value)))

				if _, werr := sstFile.Write(header); werr != nil {
					iterErr = werr
					return false
				}
				if _, werr := sstFile.Write([]byte(key)); werr != nil {
					iterErr = werr
					return false
				}
				if _, werr := sstFile.Write(value); werr != nil {
					iterErr = werr
					return false
				}
			}
			return true
		})
		return iterErr
	}()

	if err != nil {
		cleanupNewWal()
		return fmt.Errorf("migration error: %w", err)
	}

	// 4. Swap old with new
	oldWal := s.log
	s.mem = newMem
	s.log = newWal

	if oldWal != nil {
		if cerr := oldWal.Close(); cerr != nil {
			_ = cerr
		}

		if oldPath := oldWal.Path(); oldPath != "" {
			_ = os.Remove(oldPath)
		}
	}

	return nil
}

func (s *Store) AvgAccess() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.avgAccess
}
