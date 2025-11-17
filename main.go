package kvstore

import (
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func main() {

}
