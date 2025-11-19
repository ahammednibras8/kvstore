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
