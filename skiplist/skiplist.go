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
