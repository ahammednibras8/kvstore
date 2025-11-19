package skiplist

type Node struct {
	Key   string
	Value []byte
	Next  []*Node
}
