# Organic KV: A Self-Organizing Storage Engine

Organic KV is a high-performance, persistent key-value store built in Go. It implements a biological metabolism that continuously reorganizes itself based on real workload patterns.

Data in Organic KV does not just sit in storage — it adapts.

## Core Innovation: Organic Tiering

Traditional databases separate memory (cache) and disk (storage). Organic KV merges them into a single self-regulating engine.

### 1. Traffic Sensing
Every key tracks its own `AccessCount` using lock-free atomic counters.

### 2. Metabolic Flush
At flush time, the system computes a global **Temperature** using EWMA (Exponentially Weighted Moving Average). This represents the “hotness baseline” of the system.

### 3. Natural Selection
During flush:
- **Hot data** (> Temperature) stays in memory.
- **Cold data** (< Temperature) moves to immutable SSTables on disk.

This produces a continuous adaptive hierarchy without manual tuning or external caching.

## Architecture

### 1. Storage Engine (LSM-Tree Core)

**MemTable (Skip List)**
The in-memory structure is a probabilistic skip list with O(log n) operations, no rotations, and excellent behavior under concurrent writes.

**Frozen MemTable Snapshot**
During flush:
- Current MemTable becomes frozen.
- New writes go to a fresh MemTable.
- Frozen table is flushed in the background.
- Reads still work on both.

**WAL (Write-Ahead Log)**
Append-only binary log with:
- Length-prefix entries
- Replay on startup
- WAL rotated per flush generation
- Old WAL deleted only after successful flush

This ensures full durability and crash recovery.

### 2. SSTables (Immutable Disk Segments)

Every flush generates an immutable sorted SSTable file:
- Sorted key-value pairs
- Tombstones for deletes
- Sparse index for block-level jumps
- Bloom filter to skip entire files on key misses
- Generation numbers (`sst-N.sst`)

### 3. Read Path: Newest → Oldest

`Get()` resolves keys in a deterministic order:
1. MemTable
2. Frozen MemTable (if flush in progress)
3. SSTables (newest first):
   - Bloom filter check
   - Sparse index block jump
   - Forward scan until match or miss

The newest version always wins and there is no stale read.

### 4. Compaction (K-Way Merge)

Organic KV merges all SSTables into a single new one:
- Sorted K-way merge
- Deduplicates keys
- Applies tombstones
- Rebuilds sparse index and bloom filter
- Replaces old files atomically

This keeps disk usage under control and read latency stable.

## Range Queries

The engine implements a merging iterator across:
- MemTable
- Frozen MemTable
- All SSTables

This produces a sorted, deduplicated continuous range:
```
/scan?start=A100&end=A500
```
Useful for logs, analytics, and prefix queries.

## Concurrency and Safety

### Crash-Safe Flush
Flush uses a three-phase protocol:
1. Write SST to a temporary file (`sst-N.sst.temp`)
2. `fsync` and close
3. Atomic rename to final file

Only after successful rename:
- WAL is closed
- Old WAL is removed
- Frozen table is detached

This guarantees no half-written SSTs and no corrupted WAL generations.

### RWMutex Thread Model
- Readers use `RLock()`, allowing high concurrency.
- Writers acquire `Lock()` only for minimal critical sections.
- Flush work happens outside the lock.

## Metrics

**Endpoint:** `/metrics`

Exposes counters such as:
- `put_count`
- `get_count`
- `delete_count`
- `memtable_hits`
- `sstable_hits`
- `sstable_miss`
- `bloom_saved`
- `disk_reads`
- `flushes`
- `compactions`

Useful for dashboards, Prometheus, and debugging.

## Quick Start

### Run server
```bash
go run cmd/server/main.go
```

### Write
```bash
curl -X POST http://localhost:8080/set -d '{"key":"k","value":"v"}'
```

### Read
```bash
curl "http://localhost:8080/get?key=k"
```

### Flush
```bash
curl -X POST http://localhost:8080/flush
```

### Range query
```bash
curl "http://localhost:8080/scan?start=A1&end=A9"
```

### Metrics
```bash
curl http://localhost:8080/metrics
```

## Strategic Roadmap
- [ ] Configurable WAL fsync
- [ ] Automatic flush based on size
- [ ] Leveled compaction
- [ ] Read caching
- [ ] Snapshot API
- [ ] Backup/restore tools
