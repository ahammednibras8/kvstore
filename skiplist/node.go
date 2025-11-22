package skiplist

type Node struct {
	Key         string
	Value       []byte
	Type        byte
	Next        []*Node
	AccessCount int64
}
