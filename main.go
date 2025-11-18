package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type KVStore struct {
	mu      sync.RWMutex
	data    map[string]Entry
	logFile *os.File
}

type Entry struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
	Tombstone bool   `json:"tombstone"`
}

func NewStore() *KVStore {
	logFile, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	kv := &KVStore{
		data:    make(map[string]Entry),
		logFile: logFile,
	}

	kv.Recover()

	return kv
}

func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := Entry{
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Tombstone: false,
	}
	s.data[key] = entry

	walEntry := map[string]interface{}{
		"op":        "set",
		"key":       key,
		"value":     entry.Value,
		"timestamp": entry.Timestamp,
		"tombstone": entry.Tombstone,
	}

	s.appendLog(walEntry)
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok || entry.Tombstone {
		return "", false
	}

	return entry.Value, true
}

func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := Entry{
		Value:     "",
		Timestamp: time.Now().UnixNano(),
		Tombstone: true,
	}

	s.data[key] = entry

	walEntry := map[string]interface{}{
		"op":        "delete",
		"key":       key,
		"timestamp": entry.Timestamp,
		"tombstone": entry.Tombstone,
	}

	s.appendLog(walEntry)
}

func (s *KVStore) GetAll() map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copyMap := make(map[string]Entry, len(s.data))

	for k, v := range s.data {
		copyMap[k] = v
	}

	return copyMap
}

func (s *KVStore) Save(filename string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jsonData, err := json.MarshalIndent(s.data, "", " ")
	if err != nil {
		return err
	}

	err = os.WriteFile(filename, jsonData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (s *KVStore) Load(filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	temp := make(map[string]Entry)
	err = json.Unmarshal(jsonData, &temp)
	if err != nil {
		return err
	}

	s.data = temp

	return nil
}

func (s *KVStore) appendLog(entry map[string]interface{}) {
	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		return
	}

	s.logFile.Write(append(jsonBytes, '\n'))
}

func (s *KVStore) Recover() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := os.Stat("data.json"); err == nil {
		snapshotBytes, err := os.ReadFile("data.json")
		if err == nil {
			var snapshot map[string]Entry
			if err := json.Unmarshal(snapshotBytes, &snapshot); err == nil {
				s.data = snapshot
			}
		}
	}

	file, err := os.Open("wal.log")
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()

		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		op, _ := entry["op"].(string)
		key, _ := entry["key"].(string)

		var ts int64
		if t, ok := entry["timestamp"].(float64); ok {
			ts = int64(t)
		}

		switch op {
		case "set":
			val, _ := entry["value"].(string)

			s.data[key] = Entry{
				Value:     val,
				Timestamp: ts,
				Tombstone: false,
			}
		case "delete":
			s.data[key] = Entry{
				Value:     "",
				Timestamp: ts,
				Tombstone: true,
			}
		}
	}
}

func (s *KVStore) Compact() error {
	s.mu.Lock()

	snapshot := make(map[string]Entry, len(s.data))
	for k, v := range s.data {
		snapshot[k] = v
	}

	s.logFile.Close()

	if err := os.Rename("wal.log", "wal.log.old"); err != nil {
		s.mu.Unlock()
		return err
	}

	newLog, err := os.OpenFile("wal.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	s.logFile = newLog

	s.mu.Unlock()

	go func(snapshotCopy map[string]Entry) {
		jsonBytes, err := json.MarshalIndent(snapshotCopy, "", " ")
		if err == nil {
			_ = os.WriteFile("data.json", jsonBytes, 0644)
		}

		_ = os.Remove("wal.log.old")
	}(snapshot)

	return nil
}

func main() {
	store := NewStore()

	store.Set("CEO", "Ahammed Nibras")
	store.Delete("CEO")

	value, ok := store.Get("CEO")
	if !ok {
		fmt.Println("CEO not found (correct, deleted)")
	} else {
		fmt.Println("CEO:", value)
	}

	store.Set("a", "apple")
	store.Set("b", "banana")
	store.Delete("a")

	fmt.Println("\nFinal Store State:")
	for k, v := range store.GetAll() {
		fmt.Println(k, "=>", v)
	}

	fmt.Println("\nRunning Compaction...")
	if err := store.Compact(); err != nil {
		fmt.Println("Compaction error:", err)
	} else {
		fmt.Println("Compaction complete! WAL rotated.")
	}
}
