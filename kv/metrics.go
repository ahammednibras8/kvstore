package kv

import (
	"encoding/json"
	"sync/atomic"
)

type Metrics struct {
	// 1. Traffic
	PutCount    int64
	GetCount    int64
	DeleteCount int64

	// 2. Performance / Tiering
	MemtableHits int64
	SSTableHits  int64
	SSTableMiss  int64

	// 3. Optimization Effectiveness
	BloomHits int64
	DiskReads int64

	// 4. Maintenance
	Flushes     int64
	Compactions int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) ToJSON() []byte {
	data := map[string]int64{
		"put_count":    atomic.LoadInt64(&m.PutCount),
		"get_count":    atomic.LoadInt64(&m.GetCount),
		"delete_count": atomic.LoadInt64(&m.DeleteCount),

		"memtable_hits": atomic.LoadInt64(&m.MemtableHits),
		"sstable_hits":  atomic.LoadInt64(&m.SSTableHits),
		"sstable_miss":  atomic.LoadInt64(&m.SSTableMiss),

		"bloom_saved": atomic.LoadInt64(&m.BloomHits),
		"disk_reads":  atomic.LoadInt64(&m.DiskReads),

		"flushes":     atomic.LoadInt64(&m.Flushes),
		"compactions": atomic.LoadInt64(&m.Compactions),
	}

	b, _ := json.MarshalIndent(data, "", " ")
	return b
}
