package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	store := NewStore()

	store.Set("test_key", "test_value")

	value, ok := store.Get("test_key")
	if !ok {
		t.Errorf("Expected key to exist, but key was not found")
	}

	expected := "test_value"
	if value != expected {
		t.Errorf("Expected %s, got %s", expected, value)
	}
}

func TestConcurrentWrites(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)

		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key_%d", i)
			val := fmt.Sprintf("val_%d", i)
			store.Set(key, val)
		}()
	}

	wg.Wait()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		_, ok := store.Get(key)
		if !ok {
			t.Errorf("Missing key after concurrent write: %s", key)
		}
	}
}
