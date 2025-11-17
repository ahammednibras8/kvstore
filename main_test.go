package main

import "testing"

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
