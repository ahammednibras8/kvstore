package kv

import (
	"kvstore/skiplist"
	"kvstore/wal"
)

type Store struct {
	mem *skiplist.SkipList
	log *wal.WAL
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
		mem: mem,
		log: w,
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
