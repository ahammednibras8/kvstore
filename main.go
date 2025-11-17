package main

import (
	"fmt"
	"sync"
	"time"
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

func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}

func main() {
	store := NewStore()

	store.Set("CEO", "Ahammed Nibras")

	value, ok := store.Get("CEO")

	if ok {
		fmt.Println("CEO:", value)
	} else {
		fmt.Println("Key not found")
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		i := i

		wg.Add(1)

		go func() {
			defer wg.Done()

			key := fmt.Sprintf("key%d", i)
			val := fmt.Sprintf("value%d", i)
			store.Set(key, val)
		}()
	}

	wg.Wait()

	fmt.Println("Final KVStore contents:")
	for k, v := range store.data {
		fmt.Println(k, "=", v)
	}
}
