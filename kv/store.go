package kv

import (
	"container/heap"
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
	"sync/atomic"
)

type Store struct {
	mem           *skiplist.SkipList
	log           *wal.WAL
	avgAccess     float64
	alpha         float64
	mu            sync.RWMutex
	walGen        int64
	sstGen        int64
	sstFiles      []string
	sstDir        string
	sparseIndexes map[string][]SparseIndexEntry
	bloomFilter   map[string]*BloomFilter
	metrics       *Metrics
}

type HeapNode struct {
	Key       string
	Value     []byte
	Type      byte
	FileIndex int
	Gen       int64
}

type EntryHeap []*HeapNode

func (h EntryHeap) Len() int { return len(h) }
func (h EntryHeap) Less(i, j int) bool {
	if h[i].Key != h[j].Key {
		return h[i].Key < h[j].Key
	}
	return h[i].FileIndex < h[j].FileIndex
}
func (h EntryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *EntryHeap) Push(x interface{}) {
	*h = append(*h, x.(*HeapNode))
}

func (h *EntryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
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
		if e.Type == 1 {
			mem.Delete(string(e.Key))
			return nil
		}
		mem.Put(string(e.Key), e.Value)
		return nil
	})
	if err != nil {
		return nil, err
	}

	s := &Store{
		mem:           mem,
		log:           w,
		avgAccess:     0,
		alpha:         0.4,
		sstFiles:      []string{},
		sstGen:        0,
		sparseIndexes: make(map[string][]SparseIndexEntry),
		bloomFilter:   make(map[string]*BloomFilter),
		metrics:       NewMetrics(),
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

	// Track highest generation
	for _, f := range files {
		g := parseGen(f)
		if g > s.sstGen {
			s.sstGen = g
		}
	}

	log.Printf("Loaded %d SST files (newest→oldest). Highest generation = %d", len(s.sstFiles), s.sstGen)

	// REBUILD bloom filter + sparse index for every SST file
	for _, filename := range files {
		f, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("open sst %s: %w", filename, err)
		}

		// Conservative estimate: size / 100 bytes per record
		info, _ := f.Stat()
		estimatedN := int(info.Size() / 100)
		if estimatedN < 1 {
			estimatedN = 1
		}
		bf := NewBloomFilter(estimatedN, 0.01)

		var sparse []SparseIndexEntry
		const blockSize int64 = 1024
		var lastIndexPos int64 = 0

		offset := int64(0)

		for {
			header := make([]byte, 16)
			_, err := io.ReadFull(f, header)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("sst read header: %w", err)
			}

			keyLen := binary.LittleEndian.Uint64(header[0:8])
			valLen := binary.LittleEndian.Uint64(header[8:16])

			var typ [1]byte
			if _, err := io.ReadFull(f, typ[:]); err != nil {
				f.Close()
				return nil, err
			}

			keyBytes := make([]byte, keyLen)
			if _, err := io.ReadFull(f, keyBytes); err != nil {
				f.Close()
				return nil, err
			}
			key := string(keyBytes)

			// Skip Value
			if _, err := f.Seek(int64(valLen), io.SeekCurrent); err != nil {
				f.Close()
				return nil, err
			}

			// Add to Bloom Filter (tombstones INCLUDED)
			bf.Add(key)

			// Sparse index insertion
			if offset-lastIndexPos >= blockSize {
				sparse = append(sparse, SparseIndexEntry{
					Key:    key,
					Offset: offset,
				})
				lastIndexPos = offset
			}

			offset, _ = f.Seek(0, io.SeekCurrent)
		}

		s.bloomFilter[filename] = bf
		s.sparseIndexes[filename] = sparse
		f.Close()
	}

	return s, nil
}

func (s *Store) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.metrics != nil {
		atomic.AddInt64(&s.metrics.PutCount, 1)
	}

	err := s.log.Write(wal.Entry{
		Type:  0,
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		return err
	}

	s.mem.Put(key, value)
	return nil
}

func (s *Store) Get(key string) ([]byte, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.metrics != nil {
		atomic.AddInt64(&s.metrics.GetCount, 1)
	}

	// 1. Check the MemTable
	if val, hits, found := s.mem.Get(key); found {
		if s.metrics != nil {
			atomic.AddInt64(&s.metrics.MemtableHits, 1)
		}
		return val, hits, true
	}

	// 2. Check SSTables already sorted
	for _, filename := range s.sstFiles {
		if val, found := s.readFromSSTable(filename, key); found {
			s.mem.PutSurvivor(key, val, 0, 1)
			return val, 1, true
		}
	}

	return nil, 0, false
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.metrics != nil {
		atomic.AddInt64(&s.metrics.Flushes, 1)
	}

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

	// Create Bloom Filter
	estimatedN := int(count)
	if estimatedN < 1 {
		estimatedN = 1
	}
	bf := NewBloomFilter(estimatedN, 0.01)

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

	// sparse index setup
	var sparse []SparseIndexEntry
	var lastIndexPos int64 = 0
	const blockSize int64 = 1024

	var lastKeyWritten string

	// 3. Iterate through OLD memtable and migrate
	err = func() error {
		var iterErr error
		s.mem.Iterator(func(key string, value []byte, typ byte, accessCount int64) bool {
			if key == lastKeyWritten {
				return true
			}
			lastKeyWritten = key

			if float64(accessCount) > s.avgAccess {
				// Momentum Decay
				decayFactor := 0.5
				decayHits := max(int64(float64(accessCount)*decayFactor), 1)

				// Insert into new MemTable with momentum
				if typ == 1 {
					newMem.PutSurvivor(key, nil, 1, decayHits)
				} else {
					newMem.PutSurvivor(key, value, 0, decayHits)
				}

				if werr := newWal.Write(wal.Entry{
					Type:  typ,
					Key:   []byte(key),
					Value: value,
				}); werr != nil {
					iterErr = werr
					return false
				}
				return true
			}

			// Before wrinting entry get offset
			offset, _ := sstTmp.Seek(0, io.SeekCurrent)
			if offset-lastIndexPos >= blockSize {
				sparse = append(sparse, SparseIndexEntry{
					Key:    key,
					Offset: offset,
				})
				lastIndexPos = offset
			}

			// Add Key to Bloom Fileter
			bf.Add(key)

			// Write SST Enrtry
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
		return fmt.Errorf("rename tmp sst: %w", err)
	}

	s.sparseIndexes[finalSST] = sparse
	// Save the bloom Fileter for SST file
	s.bloomFilter[finalSST] = bf
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

func (s *Store) readFromSSTable(filename, targetKey string) ([]byte, bool) {
	// 1. Open the SSTable (read-only)
	f, err := os.Open(filename)
	if err != nil {
		return nil, false
	}
	defer f.Close()

	// Bloom Filter Check
	bf := s.bloomFilter[filename]
	if bf != nil {
		if !bf.Contains(targetKey) {
			if s.metrics != nil {
				atomic.AddInt64(&s.metrics.BloomHits, 1)
			}
			return nil, false
		}
	}

	// 2. LookUp sparse index for this file
	sparse := s.sparseIndexes[filename]

	// 3. If Index exists , binary-search to find the correct block
	if len(sparse) > 0 {
		idx := sort.Search(len(sparse), func(i int) bool {
			return sparse[i].Key > targetKey
		})

		if idx > 0 {
			idx = idx - 1
		} else {
			idx = 0
		}

		off := sparse[idx].Offset

		if s.metrics != nil {
			atomic.AddInt64(&s.metrics.DiskReads, 1)
		}

		// 4. Seek to the start of that block
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			log.Printf("seek failed: %v", err)
			return nil, false
		}
	} else {
		// No sparse index — we will read from start. Count as disk read.
		if s.metrics != nil {
			atomic.AddInt64(&s.metrics.DiskReads, 1)
		}

		if _, err := f.Seek(0, io.SeekStart); err != nil {
			log.Printf("seek to start failed: %v", err)
			return nil, false
		}
	}

	// 5. Scan Forward normally
	header := make([]byte, 16)

	for {
		_, err := io.ReadFull(f, header)
		if err == io.EOF {
			if s.metrics != nil {
				atomic.AddInt64(&s.metrics.SSTableMiss, 1)
			}
			return nil, false
		}
		if err != nil {
			log.Printf("SST read error (header): %v", err)
			return nil, false
		}

		keyLen := binary.LittleEndian.Uint64(header[0:8])
		valLen := binary.LittleEndian.Uint64(header[8:16])

		var typ [1]byte
		_, err = io.ReadFull(f, typ[:])
		if err != nil {
			log.Printf("SST read error (type): %v", err)
			return nil, false
		}

		keyBytes := make([]byte, keyLen)
		_, err = io.ReadFull(f, keyBytes)
		if err != nil {
			log.Printf("SST read error (key): %v", err)
			return nil, false
		}
		key := string(keyBytes)

		// 5. Early stopping: SST keys are sorted
		if key > targetKey {
			if s.metrics != nil {
				atomic.AddInt64(&s.metrics.SSTableMiss, 1)
			}
			return nil, false
		}

		// 6. If Match found
		if key == targetKey {
			if typ[0] == 1 {
				if s.metrics != nil {
					atomic.AddInt64(&s.metrics.SSTableMiss, 1)
				}
				return nil, false
			}

			value := make([]byte, valLen)
			_, err = io.ReadFull(f, value)
			if err != nil {
				log.Printf("SST read error (value): %v", err)
				return nil, false
			}

			if s.metrics != nil {
				atomic.AddInt64(&s.metrics.SSTableHits, 1)
			}

			return value, true
		}

		_, err = f.Seek(int64(valLen), io.SeekCurrent)
		if err != nil {
			log.Printf("SST seek error: %v", err)
			return nil, false
		}
	}
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.metrics != nil {
		atomic.AddInt64(&s.metrics.DeleteCount, 1)
	}

	err := s.log.Write(wal.Entry{
		Type:  1,
		Key:   []byte(key),
		Value: nil,
	})
	if err != nil {
		return err
	}

	s.mem.PutSurvivor(key, nil, 1, 0)

	return nil
}

func readNextEntry(f *os.File) (*HeapNode, error) {
	header := make([]byte, 16)

	_, err := io.ReadFull(f, header)
	if err != nil {
		return nil, err
	}

	keyLen := binary.LittleEndian.Uint64(header[0:8])
	valLen := binary.LittleEndian.Uint64(header[8:16])

	var typ [1]byte
	if _, err := io.ReadFull(f, typ[:]); err != nil {
		return nil, err
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(f, key); err != nil {
		return nil, err
	}

	value := make([]byte, valLen)
	if valLen > 0 {
		if _, err := io.ReadFull(f, value); err != nil {
			return nil, err
		}
	}

	return &HeapNode{
		Key:   string(key),
		Value: value,
		Type:  typ[0],
	}, nil
}

func writeSSTEntry(out *os.File, n *HeapNode) error {
	header := make([]byte, 16)
	binary.LittleEndian.PutUint64(header[0:8], uint64(len(n.Key)))
	binary.LittleEndian.PutUint64(header[8:16], uint64(len(n.Value)))

	if _, err := out.Write(header); err != nil {
		return err
	}
	if _, err := out.Write([]byte{n.Type}); err != nil {
		return err
	}
	if _, err := out.Write([]byte(n.Key)); err != nil {
		return err
	}
	if _, err := out.Write(n.Value); err != nil {
		return err
	}

	return nil
}

func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.metrics != nil {
		atomic.AddInt64(&s.metrics.Compactions, 1)
	}

	inputFiles := s.sstFiles
	if len(inputFiles) < 2 {
		return nil
	}

	// 1. Estimate Bloom Filter size
	var totalSize int64
	for _, f := range inputFiles {
		info, err := os.Stat(f)
		if err == nil {
			totalSize += info.Size()
		}
	}

	// Assume ~100 bytes per entry
	estimatedN := int(totalSize / 100)
	if estimatedN < 1 {
		estimatedN = 1
	}

	// 2. Open SST files and build initial heap
	readers := make([]*os.File, len(inputFiles))
	h := &EntryHeap{}
	heap.Init(h)

	for i, filename := range inputFiles {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		readers[i] = f

		if node, err := readNextEntry(f); err == nil {
			node.FileIndex = i
			node.Gen = int64(i)
			heap.Push(h, node)
		}
	}

	// 3. Create output SST (temp)
	s.sstGen++
	outName := fmt.Sprintf("sst-%d.sst", s.sstGen)
	tmpName := outName + ".tmp"

	out, err := os.Create(tmpName)
	if err != nil {
		return err
	}
	defer out.Close()

	// Create Bloom Filter for new file
	bf := NewBloomFilter(estimatedN, 0.01)

	var lastkey string

	// Sparse Index Setup
	var sparse []SparseIndexEntry
	var lastIndexPos int64 = 0
	const blockSize int64 = 1024

	// 4. K-way merge loop
	for h.Len() > 0 {
		minNode := heap.Pop(h).(*HeapNode)

		if minNode.Key != lastkey {
			// Add key to Bloom Filter — EVEN tombstones
			bf.Add(minNode.Key)

			if minNode.Type == 0 {
				offset, _ := out.Seek(0, io.SeekCurrent)
				if offset-lastIndexPos >= blockSize {
					sparse = append(sparse, SparseIndexEntry{
						Key:    minNode.Key,
						Offset: offset,
					})
					lastIndexPos = offset
				}

				if err := writeSSTEntry(out, minNode); err != nil {
					return err
				}
			}
			lastkey = minNode.Key
		}

		if nextNode, err := readNextEntry(readers[minNode.FileIndex]); err == nil {
			nextNode.FileIndex = minNode.FileIndex
			nextNode.Gen = minNode.Gen
			heap.Push(h, nextNode)
		}
	}

	// 5. Sync + close output
	if err := out.Sync(); err != nil {
		return err
	}
	out.Close()

	// 6. Replace SST list
	if err := os.Rename(tmpName, outName); err != nil {
		return err
	}

	oldFile := inputFiles
	s.sstFiles = []string{outName}

	// Save Sparse Index
	s.sparseIndexes[outName] = sparse

	s.bloomFilter[outName] = bf

	// 7. Delete all old SST files
	for _, f := range oldFile {
		if err := os.Remove(f); err != nil {
			log.Printf("warning: failed to delete old SST %s: %v", f, err)
		}

		// Remove stale metadata to prevent memory leak
		delete(s.sparseIndexes, f)
		delete(s.bloomFilter, f)
	}

	return nil
}

func (s *Store) Metrics() *Metrics {
	return s.metrics
}
