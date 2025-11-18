package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type KVStore struct {
	mu      sync.RWMutex
	data    map[string]string
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
		data:    make(map[string]string),
		logFile: logFile,
	}

	kv.Recover()

	return kv
}

func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value

	entry := map[string]string{
		"op":    "set",
		"key":   key,
		"value": value,
	}
	s.appendLog(entry)
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

	entry := map[string]string{
		"op":  "delete",
		"key": key,
	}
	s.appendLog(entry)
}

func (s *KVStore) GetAll() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copyMap := make(map[string]string, len(s.data))

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

	temp := make(map[string]string)
	err = json.Unmarshal(jsonData, &temp)
	if err != nil {
		return err
	}

	s.data = temp

	return nil
}

func (s *KVStore) appendLog(entry map[string]string) {
	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		return
	}

	s.logFile.Write(append(jsonBytes, '\n'))
}

func (s *KVStore) Recover() {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open("wal.log")
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()

		var entry map[string]string
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		op := entry["op"]
		key := entry["key"]

		switch op {
		case "set":
			s.data[key] = entry["value"]
		case "delete":
			delete(s.data, key)
		}
	}
}

func main() {
	store := NewStore()

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
	all := store.GetAll()

	for k, v := range all {
		fmt.Println(k, "=", v)
	}

	// Save the Data to Disk
	err := store.Save("data.json")
	if err != nil {
		fmt.Println("Error saving:", err)
	} else {
		fmt.Println("Data saved to data.json")
	}

	//Load Data to Memory
	newStore := NewStore()

	err = newStore.Load("data.json")
	if err != nil {
		fmt.Println("Load Error:", err)
	} else {
		fmt.Println("Loaded from disk:", newStore.GetAll())
	}
}
