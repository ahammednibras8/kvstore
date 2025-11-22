package kv

import (
	"encoding/binary"
	"fmt"
	"io"
	"kvstore/skiplist"
	"kvstore/wal"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Store struct {
	mem       *skiplist.SkipList
	log       *wal.WAL
	avgAccess float64
	alpha     float64
	mu        sync.RWMutex
	walGen    int64
	sstGen    int64
	sstFiles  []string
	sstDir    string
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

	s := &Store{
		mem:       mem,
		log:       w,
		avgAccess: 0,
		alpha:     0.4,
		sstFiles:  []string{},
		sstGen:    0,
	}

	// 4. Discover existing SST files
	files, err := filepath.Glob("sst-*.sst")
	if err != nil {
		return nil, fmt.Errorf("glob sst files: %w", err)
	}

	parseGen := func(name string) int64 {
		var g int64
		if _, err := fmt.Sscanf(name, "sst-%d.sst", &g); err == nil {
			return g
		}
		return -1
	}

	// 5. Sort by generation newest to oldest
	sort.Slice(files, func(i, j int) bool {
		return parseGen(files[i]) > parseGen(files[j])
	})

	// 6. Determine Heighest generation
	for _, f := range files {
		gen := parseGen(f)
		if gen > s.sstGen {
			s.sstGen = gen
		}
	}

	s.sstFiles = files

	log.Printf("Loaded %d SST files (newest→oldest). Highest generation = %d", len(s.sstFiles), s.sstGen)

	return s, nil
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

	// 1. Check the MemTable
	if val, found := s.mem.Get(key); found {
		return val, true
	}

	// 2. Check SSTables already sorted
	for _, filename := range s.sstFiles {
		if val, found := s.readFromSSTable(filename, key); found {
			return val, true
		}
	}

	return nil, false
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A. Calculate instantaneous average access count
	var sum float64
	var count float64

	s.mem.Iterator(func(key string, value []byte, typ byte, accessCount int64) bool {
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

	// 2. Create SST filename (level 0 style)
	s.sstGen++
	finalSST := fmt.Sprintf("sst-%d.sst", s.sstGen)
	tmpSST := finalSST + ".temp"

	sstTmp, err := os.OpenFile(tmpSST, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		cleanupNewWal()
		return fmt.Errorf("open tmp sst: %w", err)
	}

	// 3. Iterate through OLD memtable and migrate
	err = func() error {
		var iterErr error
		s.mem.Iterator(func(key string, value []byte, typ byte, accessCount int64) bool {
			if float64(accessCount) > s.avgAccess {
				newMem.Put(key, value)
				if werr := newWal.Write(wal.Entry{
					Type:  typ,
					Key:   []byte(key),
					Value: value,
				}); werr != nil {
					iterErr = werr
					return false
				}
				return true
			} else {
				header := make([]byte, 16)
				binary.LittleEndian.PutUint64(header[0:8], uint64(len(key)))
				binary.LittleEndian.PutUint64(header[8:16], uint64(len(value)))

				// a. write header
				if _, werr := sstTmp.Write(header); werr != nil {
					iterErr = werr
					return false
				}

				// b. write Type byte (0 = PUT, 1 = DELETE)
				if _, werr := sstTmp.Write([]byte{typ}); werr != nil {
					iterErr = werr
					return false
				}

				// c. write Key
				if _, werr := sstTmp.Write([]byte(key)); werr != nil {
					iterErr = werr
					return false
				}

				// d. write Value
				if value != nil {
					if _, werr := sstTmp.Write(value); werr != nil {
						iterErr = werr
						return false
					}
				}
			}
			return true
		})
		return iterErr
	}()

	if err != nil {
		cleanupNewWal()
		_ = sstTmp.Close()
		_ = os.Remove(tmpSST)
		return fmt.Errorf("migration error: %w", err)
	}

	// 4. Ensure temp file is flushed to disk and closed
	if err := sstTmp.Sync(); err != nil {
		cleanupNewWal()
		_ = sstTmp.Close()
		_ = os.Remove(tmpSST)
		return fmt.Errorf("sync tmp sst: %w", err)
	}
	if err := sstTmp.Close(); err != nil {
		cleanupNewWal()
		_ = os.Remove(tmpSST)
		return fmt.Errorf("close tmp sst: %w", err)
	}

	// 5. Atomically replace store.sst with the temp file
	if err := os.Rename(tmpSST, finalSST); err != nil {
		cleanupNewWal()
		_ = os.Remove(tmpSST)
		return fmt.Errorf("rename tmp sst: %w", err)
	}
	s.sstFiles = append([]string{finalSST}, s.sstFiles...)

	// 6. Swap old WAL/mem with new ones (we hold the store lock)
	oldWal := s.log
	s.mem = newMem
	s.log = newWal

	if oldWal != nil {
		if cerr := oldWal.Close(); cerr != nil {
			log.Printf("warning: failed to close old wal: %v", cerr)
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

func (s *Store) readFromSSTable(filename, key string) ([]byte, bool) {
	// 1. Open the SSTable (read-only)
	f, err := os.Open(filename)
	if err != nil {
		return nil, false
	}
	defer f.Close()

	header := make([]byte, 16)

	for {
		// 2. Read header: keyLen + valLen
		_, err := io.ReadFull(f, header)
		if err == io.EOF {
			return nil, false
		}
		if err == io.ErrUnexpectedEOF {
			log.Printf("SSTable corruption: partial header encountered - stopping parse")
			return nil, false
		}
		if err != nil {
			log.Printf("SSTable read error (header): %v", err)
			return nil, false
		}

		keyLen := binary.LittleEndian.Uint64(header[0:8])
		valLen := binary.LittleEndian.Uint64(header[8:16])

		// 3. Read Type byte
		var typ [1]byte
		_, err = io.ReadFull(f, typ[:])
		if err != nil {
			log.Printf("SSTable read error (type): %v", err)
			return nil, false
		}

		// 4. Read the key
		keyBytes := make([]byte, keyLen)
		_, err = io.ReadFull(f, keyBytes)
		if err != nil {
			log.Printf("SSTable read error (key): %v", err)
			return nil, false
		}

		// 5. Compare to target key
		if string(keyBytes) == key {
			if typ[0] == 1 {
				return nil, false
			}

			value := make([]byte, valLen)
			_, err = io.ReadFull(f, value)
			if err != nil {
				log.Printf("SSTable read error (value): %v", err)
				return nil, false
			}
			return value, true
		}

		// 6. No Match -> Skip the value bytes
		_, err = f.Seek(int64(valLen), io.SeekCurrent)
		if err != nil {
			log.Printf("SSTable seek error: %v", err)
			return nil, false
		}
	}
}
