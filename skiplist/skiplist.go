package skiplist

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type SkipList struct {
	Head     *Node
	MaxLevel int
	Level    int
	P        float64
	rng      *rand.Rand
}

func NewSkipList(p float64, maxLevel int) *SkipList {
	head := &Node{
		Key:   "",
		Value: nil,
		Next:  make([]*Node, maxLevel),
	}

	src := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(src)

	return &SkipList{
		Head:     head,
		MaxLevel: maxLevel,
		Level:    1,
		P:        p,
		rng:      rng,
	}
}

func (s *SkipList) randomLevel() int {
	level := 1

	for s.rng.Float64() < s.P && level < s.MaxLevel {
		level++
	}

	return level
}

func (s *SkipList) Put(key string, value []byte) {
	update := make([]*Node, s.MaxLevel)

	current := s.Head

	// 1. Find insert positions (predecessors)
	for lvl := s.Level - 1; lvl >= 0; lvl-- {
		for current.Next[lvl] != nil && current.Next[lvl].Key < key {
			current = current.Next[lvl]
		}
		update[lvl] = current
	}

	next := current.Next[0]

	// 2. If the key already exists, replace the value
	if next != nil && next.Key == key {
		next.Value = value
		return
	}

	// 3. Determine the height of the new node
	newLevel := s.randomLevel()

	// 4. If new node is taller, initialize update[] for high levels
	if newLevel > s.Level {
		for lvl := s.Level; lvl < newLevel; lvl++ {
			update[lvl] = s.Head
		}
		s.Level = newLevel
	}

	// 5. Create the new node with 'newLevel' height
	newNode := &Node{
		Key:   key,
		Value: value,
		Next:  make([]*Node, newLevel),
	}

	// 6. Stitch in the new node at all levels
	for lvl := 0; lvl < newLevel; lvl++ {
		newNode.Next[lvl] = update[lvl].Next[lvl]
		update[lvl].Next[lvl] = newNode
	}
}

func (s *SkipList) Get(key string) ([]byte, bool) {
	current := s.Head

	for lvl := s.Level - 1; lvl >= 0; lvl-- {
		for current.Next[lvl] != nil && current.Next[lvl].Key < key {
			current = current.Next[lvl]
		}
	}

	next := current.Next[0]

	if next != nil && next.Key == key {
		atomic.AddInt64(&next.AccessCount, 1)
		return next.Value, true
	}

	return nil, false
}

func (s *SkipList) Iterator(fn func(key string, value []byte, accessCount int64) bool) {
	current := s.Head.Next[0]

	for current != nil {
		if !fn(current.Key, current.Value, current.AccessCount) {
			return
		}

		current = current.Next[0]
	}
}
