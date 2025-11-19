package skiplist

type SkipList struct {
	Head     *Node
	MaxLevel int
	Level    int
	P        float64
}

func NewSkipList(p float64, maxLevel int) *SkipList {
	head := &Node{
		Key:   "",
		Value: nil,
		Next:  make([]*Node, maxLevel),
	}

	return &SkipList{
		Head:     head,
		MaxLevel: maxLevel,
		Level:    1,
		P:        p,
	}
}
