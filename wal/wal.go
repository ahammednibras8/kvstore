package wal

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	offset int64
}

type Entry struct {
	Key   []byte
	Value []byte
}

func Open(path string) (*WAL, error) {
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND

	f, err := os.OpenFile(path, flags, 0644)
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
		path:   path,
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

	// 2. Bundle header + key + value
	record := append(header, entry.Key...)
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
		_, err := w.file.Read(header)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// 2. Decode keyLen and valLen
		keyLen := binary.LittleEndian.Uint64(header[0:8])
		valLen := binary.LittleEndian.Uint64(header[8:16])

		// 3. Read the key
		key := make([]byte, keyLen)
		_, err = io.ReadFull(w.file, key)
		if err != nil {
			return err
		}

		// 4. Read the value
		value := make([]byte, valLen)
		_, err = io.ReadFull(w.file, value)
		if err != nil {
			return err
		}

		// 5. Callback with parsed entry
		entry := Entry{Key: key, Value: value}
		if err := fn(entry); err != nil {
			return err
		}
	}
}
