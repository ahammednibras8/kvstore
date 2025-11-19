package skiplist

type Node struct {
	Key         string
	Value       []byte
	Next        []*Node
	AccessCount int64
}
