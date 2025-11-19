package kv

import (
	"encoding/binary"
	"kvstore/skiplist"
	"kvstore/wal"
	"os"
)

type Store struct {
	mem       *skiplist.SkipList
	log       *wal.WAL
	avgAccess float64
	alpha     float64
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
	return s.mem.Get(key)
}

func (s *Store) Flush() error {
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

	newWal, err := wal.Open("wal.next")
	if err != nil {
		return err
	}

	// 2. Open SSTable file for cold nodes
	sstFile, err := os.OpenFile("store.sst", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer sstFile.Close()

	// 3. Iterate through OLD memtable
	s.mem.Iterator(func(key string, value []byte, accessCount int64) bool {
		if float64(accessCount) > s.avgAccess {
			newMem.Put(key, value)
			_ = newWal.Write(wal.Entry{
				Key:   []byte(key),
				Value: value,
			})
		} else {
			header := make([]byte, 16)
			binary.LittleEndian.PutUint64(header[0:8], uint64(len(key)))
			binary.LittleEndian.PutUint64(header[8:16], uint64(len(value)))

			sstFile.Write(header)
			sstFile.Write([]byte(key))
			sstFile.Write(value)
		}

		return true
	})

	// 4. Swap old with new
	s.mem = newMem
	s.log = newWal

	return nil
}
