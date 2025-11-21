# Organic KV: A Self-Organizing Storage Engine

Organic KV is a high-performance, persistent key-value store built in Go.
Instead of treating all data equally, it implements a biological metabolism that continuously reorganizes the system based on real workload patterns.

Data doesn’t just live here — it adapts.

## 🧠 The Core Innovation: Organic Tiering

Traditional databases bolt caching layers on top of storage (Redis → Postgres).
Organic KV collapses this into a single self-regulating engine:

1. **Traffic Sensing** — Every key tracks its own AccessCount via atomic counters.own `AccessCount` using atomic counters (lock-free reads).
2. **Metabolic Flush** — At flush time, the system computes a global “Temperature” using an Exponentially Weighted Moving Average (EWMA).
3. **Natural Selection**

   - **Hot Data (> Temperature)** stays in memory.
   - **Cold Data (< Temperature)** is pushed to immutable SSTables on disk.

   The result: a storage engine that performs continuous internal optimization without external caches, heuristics, or administrators.

## 🏗 Architecture

### 1. Storage Engine (LSM-Tree Core)

- **MemTable:** A probabilistic Skip List with O(log n) performance. Chosen over Red-Black Trees to reduce structural rotations and global lock contention under concurrent writes.

- **Tiered Read Path:** Get() first checks the MemTable, then scans SSTables from newest → oldest, guaranteeing:
  - The newest value always wins.
  - No “time-travel reads.”
  - Deterministic cold-data lookup.
- **WAL (Write-Ahead Log):** Append-only, length-prefixed binary log to guarantee recoverability with minimal overhead.

### 2. Concurrency & Safety

- **Crash-Safe Atomic Flush:** SSTables are written to <name>.temp, then:
  1. Fully fsync’d to disk
  2. Closed
  3. Atomically renamed → final SST
     This ensures the engine never lands in a half-written state.
- **Generational File System:** Monotonic filenames (sst-7.sst) guarantee ordering, simplify recovery, and prevent accidental overwrite.
- **Thread Model:** sync.RWMutex enables high-throughput concurrent reads while maintaining strict write safety.
- **Full WAL Replay on Startup:** Recovery reconstructs the complete MemTable state of the last successful generation.

## 🚀 Quick Start

### Prerequisites

- Go 1.21+

### Running the Server

```bash
go run cmd/server/main.go
```

#### Write a Key

```
curl -X POST http://localhost:8080/set -d '{"key":"systems","value":"engineer"}'
```

#### Read a Key

```
curl "http://localhost:8080/get?key=systems"
```

#### Trigger Metabolic Flush

```
curl -X POST http://localhost:8080/flush
```

## Strategic Roadmap

- **Bloom Filters:** Eliminate unnecessary disk seeks on key misses, reducing read latency for cold data.
- **Sparse Index:** Accelerate SSTable lookups by enabling block-level binary search instead of full-file scans.
- **Autonomous Compaction:** Background merging and deduplication of SSTables to maintain consistent read latency and reclaim storage as data evolves.
