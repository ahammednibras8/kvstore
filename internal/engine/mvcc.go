package engine

type MVCCValue struct {
	Data      string
	CommitTS  int64
	Seq       uint64
	Tombstone bool
}
