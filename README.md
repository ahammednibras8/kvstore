# Organic KV: A Self-Organizing Storage Engine

A high-performance, persistent Key-Value store built in Go. Unlike standard databases that treat all data equally, Organic KV implements a biological "metabolism" that automatically tiers data based on usage patterns.

## 🧠 The Core Innovation: Organic Tiering

Most databases rely on complex, distinct caching layers (Redis + Postgres). Organic KV unifies this by implementing an internal **Survival of the Fittest** eviction policy:

1.  **Traffic Sensing:** Every node in the MemTable tracks its own `AccessCount` using atomic counters (lock-free reads).
2.  **Metabolic Flush:** During disk flushes, the system calculates the global "Temperature" using an **Exponentially Weighted Moving Average (EWMA)**.
3.  **Natural Selection:**
    - **Hot Data (> Temperature):** Survives in memory (migrated to the next generation MemTable).
    - **Cold Data (< Temperature):** Evicted to compressed disk storage (SSTable).

## 🏗 Architecture

### 1. The Storage Engine (LSM-Tree)

- **MemTable:** Implemented as a **Probabilistic Skip List** ($O(\log n)$). Chosen over Red-Black trees to reduce concurrent contention.
- **WAL (Write-Ahead Log):** Append-only durability. Uses a **Length-Prefixed** binary format to ensure crash recovery.
- **SSTable:** Immutable disk files for cold data storage.

### 2. Concurrency & Safety

- **Thread Safety:** Uses `sync.RWMutex` for high-throughput concurrent reads.
- **Generational Log Rotation:** Implements monotonic generation IDs (`wal-1.log`, `wal-2.log`) to ensure safe file rotation without locking writers.
- **Crash Recovery:** Full log replay capability on startup to restore memory state.

## 🚀 Quick Start

### Prerequisites

- Go 1.21+

### Running the Server

```bash
go mod init kvstore
go run cmd/server/main.go
```

### API Usage

#### Set a Key

```
curl -X POST http://localhost:8080/set -d '{"key":"systems","value":"engineer"}'
```

#### Get a Key

```
curl "http://localhost:8080/get?key=systems"
```

#### Trigger Metabolic Flush

```
curl -X POST http://localhost:8080/flush
```

## Future Improvements

- **Bloom Filters:** To eliminate disk seeks for non-existent keys in SSTables.
- **Sparse Index:** To optimize SSTable lookups.
- **Compaction:** Merging multiple SSTables to reclaim space.
