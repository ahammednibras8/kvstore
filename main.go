package main

import (
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func main() {
	store := NewStore()
	_ = store
}
