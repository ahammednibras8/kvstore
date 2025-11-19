package wal

import (
	"os"
	"sync"
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	offset int64
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
