package skiplist

import (
	"math/rand"
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

	for lvl := s.Level - 1; lvl >= 0; lvl-- {
		for current.Next[lvl] != nil && current.Next[lvl].Key < key {
			current = current.Next[lvl]
		}
		update[lvl] = current
	}

	next := current.Next[0]

	if next != nil && next.Key == key {
		next.Value = value
		return
	}
}
