package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	offset int64
}

type Entry struct {
	Type  byte
	Key   []byte
	Value []byte
}

func Open(path string) (*WAL, error) {
	// 1. Discover existing wal-*.log files
	walFiles, _ := filepath.Glob("wal-*.log")

	parseGen := func(name string) int64 {
		var g int64
		if _, err := fmt.Sscanf(name, "wal-%d.log", &g); err == nil {
			return g
		}
		return -1
	}

	// 2. Determine which WAL to open
	var walPath string

	if len(walFiles) == 0 {
		walPath = path
	} else {
		newest := walFiles[0]
		for _, f := range walFiles {
			if parseGen(f) > parseGen(newest) {
				newest = f
			}
		}
		walPath = newest
	}

	// 3. Open WAL file
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND

	f, err := os.OpenFile(walPath, flags, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &WAL{
		file:   f,
		path:   walPath,
		offset: info.Size(),
	}, nil
}

func (w *WAL) Write(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Build the header
	header := make([]byte, 16)
	binary.LittleEndian.PutUint64(header[0:8], uint64(len(entry.Key)))
	binary.LittleEndian.PutUint64(header[8:16], uint64(len(entry.Value)))

	// 2. Bundle header + type + key + value
	record := append(header, entry.Type)
	record = append(record, entry.Key...)
	record = append(record, entry.Value...)

	// 3. Write the file automatically
	n, err := w.file.Write(record)
	if err != nil {
		return err
	}

	// 4. Update offset
	w.offset += int64(n)

	return nil
}

func (w *WAL) Iterate(fn func(Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}

	header := make([]byte, 16)

	for {
		// 1. Read the 16-byte Header
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}

		// 2. Decode lengths
		keyLen := binary.LittleEndian.Uint64(header[0:8])
		valLen := binary.LittleEndian.Uint64(header[8:16])

		// 3. Read Type byte
		var typByte [1]byte
		_, err = io.ReadFull(w.file, typByte[:])
		if err != nil {
			return err
		}

		// 4. Read key
		key := make([]byte, keyLen)
		_, err = io.ReadFull(w.file, key)
		if err != nil {
			return err
		}

		// 5. Read the value bytes
		value := make([]byte, valLen)
		_, err = io.ReadFull(w.file, value)
		if err != nil {
			return err
		}

		// 6. Invoke callback
		entry := Entry{
			Type:  typByte[0],
			Key:   key,
			Value: value,
		}

		if err := fn(entry); err != nil {
			return err
		}
	}
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	_ = w.file.Sync()

	err := w.file.Close()
	w.file = nil
	return err
}

func (w *WAL) Path() string {
	return w.path
}
