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
		Type:  0,
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
		if next.Type == 1 {
			return nil, false
		}

		atomic.AddInt64(&next.AccessCount, 1)
		return next.Value, true
	}

	return nil, false
}

func (s *SkipList) Iterator(fn func(key string, value []byte, typ byte, accessCount int64) bool) {
	current := s.Head.Next[0]

	for current != nil {
		if !fn(current.Key, current.Value, current.Type, current.AccessCount) {
			return
		}

		current = current.Next[0]
	}
}

func (s *SkipList) Delete(key string) {
	update := make([]*Node, s.MaxLevel)
	current := s.Head

	// 1. Find predecessors
	for lvl := s.Level - 1; lvl >= 0; lvl-- {
		for current.Next[lvl] != nil && current.Next[lvl].Key < key {
			current = current.Next[lvl]
		}
		update[lvl] = current
	}

	next := current.Next[0]

	// 2. If node already exists, convert it into a tombstone
	if next != nil && next.Key == key {
		next.Type = 1
		next.Value = nil
		return
	}

	// 3. Create a new tombstone node
	newLevel := s.randomLevel()

	if newLevel > s.Level {
		for lvl := s.Level; lvl < newLevel; lvl++ {
			update[lvl] = s.Head
		}
		s.Level = newLevel
	}

	newNode := &Node{
		Key:   key,
		Type:  1,
		Value: nil,
		Next:  make([]*Node, newLevel),
	}

	// 4. Insert node into all levels
	for lvl := 0; lvl < newLevel; lvl++ {
		newNode.Next[lvl] = update[lvl].Next[lvl]
		update[lvl].Next[lvl] = newNode
	}
}
