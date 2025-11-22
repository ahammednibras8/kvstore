package main

import (
	"encoding/json"
	"fmt"
	"kvstore/kv"
	"log"
	"net/http"
)

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	// 1. Open the store
	store, err := kv.Open("wal-0.log")
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}

	log.Println("Organic KV Store is running on :8080")

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}

		var req setRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if req.Key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}

		if err := store.Put(req.Key, []byte(req.Value)); err != nil {
			http.Error(w, "failed to write", http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "OK: stored key =%s\n", req.Key)
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key required", http.StatusBadRequest)
			return
		}

		value, found := store.Get(key)
		if !found {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		log.Printf("GET key=%s | new temerature(avgAccess)=%.2f", key, store.AvgAccess())

		w.Write(value)
	})

	http.HandleFunc("/flush", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}

		if err := store.Flush(); err != nil {
			http.Error(w, "flush error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "Flush Complete.\n")
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "DELETE required", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}

		if err := store.Delete(key); err != nil {
			http.Error(w, "failed to delete key: "+err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "OK: deleted key = %s\n", key)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
