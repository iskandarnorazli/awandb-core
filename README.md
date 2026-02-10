# ☁️ AwanDB Core (OSS)

**The Embedded HTAP Database: Postgres Concurrency meets DuckDB Speed.**

AwanDB Core is an open-source, **Hybrid Transactional/Analytical Processing (HTAP)** engine. It is designed to handle high-velocity data ingestion (OLTP) and complex, blazing-fast reporting (OLAP) within a single, embedded runtime.

> **Project Vision:** Why run two databases?
>
> Typically, developers write to a transactional DB (like Postgres) and sync data to an analytical DB (like DuckDB/ClickHouse) for reporting.
>
> **AwanDB unifies this.** It accepts ACID transactions at millions of ops/sec and executes analytical queries on that same data microseconds later, using raw C++ AVX-512 intrinsics.

## 🚀 Key Features

* **True HTAP Architecture:**
    * **OLTP:** Async, actor-model ingestion (Netty-style) handles massive concurrency.
    * **OLAP:** Vectorized C++ kernels scan data at memory bandwidth limits (>100 GB/s).
* **Hybrid Scan Strategy:**
    * **Fast Path:** Blocks with no deletions are scanned via raw AVX-512 instructions (Clean Scan).
    * **Safe Path:** Blocks with active deletions use a bitmap filter to ensure Snapshot Isolation (Dirty Scan).
* **Columnar-First, Integer-Optimized:** We are an "Integer-First" database. We prioritize financial and scientific data types for maximum CPU throughput, while efficiently handling Strings via Dictionary Encoding.
* **ANSI-SQL Style Analytics:** Full support for **Hash Joins**, **Late Materialization**, and **Vectorized Aggregations**.
* **Vector Native:** Built-in support for **Vector Embeddings** (128-dim) with Cosine Similarity.

## 🗺️ OSS Roadmap (v2026.02)

We have evolved from a storage engine into a complete HTAP system.

### **Phase 0–3: The Core Engine (Completed)**
*Foundation, Storage, and Raw Compute Speed.*

| Phase | Module | Feature | Status | Impact |
| :--- | :--- | :--- | :--- | :--- |
| **0** | Memory | **Aligned Allocator** | ✅ DONE | Foundation for Zero-Copy operations. |
| **0** | Storage | **Arrow Block Layout** | ✅ DONE | Standard format for efficient data interchange. |
| **1** | Transact | **WAL (Durability)** | ✅ DONE | Data durability guarantees. |
| **2** | Storage | **Block I/O (LSM)** | ✅ DONE | Immutable disk persistence. |
| **3** | Compute | **Shared Scan** | ✅ DONE | Massive throughput scaling for concurrent queries. |
| **3** | Transact | **Write Fusion** | ✅ DONE | High-throughput batch ingestion. |

### **Phase 4–5: Types & Query Engine (Completed)**
*Handling Complex Data and execution pipelines.*

| Phase | Module | Feature | Status | Impact |
| :--- | :--- | :--- | :--- | :--- |
| **4** | Types | **German String Layout** | ✅ DONE | High-performance text filtering. |
| **5** | Compute | **Radix Sort** | ✅ DONE | 3x Faster Sorting vs Java Parallel Sort. |
| **5** | Compute | **Hybrid Scan** | ✅ DONE | Adaptive scanning for Clean vs. Dirty blocks. |
| **5** | Query | **Hash Join Operator** | ✅ DONE | **1.5x Faster** Multi-Table Joins (SIP). |

### **Phase 6: The Platform (Current Focus)**
*Networking, Graph, and Hardware Awareness.*

| Phase | Module | Feature | Status | Impact |
| :--- | :--- | :--- | :--- | :--- |
| **6** | Transact | **Atomic Updates** | ✅ DONE | Thread-safe Delete + Insert cycle. |
| **6** | Ingest | **JSON Shredder** | 📅 New | High-performance semi-structured data ingestion. |
| **6** | Network | **Arrow Flight (Tier C)** | 📅 Pending | Standard network interface for clients. |

## 📊 Performance Metrics (Verified)

*Benchmarks run on 1 Million Rows (Transactional Mode).*

AwanDB utilizes a **Hybrid Scan** strategy. Performance varies depending on whether data blocks are "Clean" (immutable/compacted) or "Dirty" (contain active deletions/updates).

### ⚡ Transactional & Update Performance
| Workload | Throughput | Bandwidth | Notes |
| :--- | :--- | :--- | :--- |
| **Seq Scan (Clean)** | **16.1 Billion Rows/s** | **123 GB/s** | Pure AVX-512 Scan (Memory Saturated) |
| **Rand Read** | **569 Million Ops/s** | **4.3 GB/s** | Point lookups via Primary Index |
| **Trans. Write** | **2.43 Million Ops/s** | **~19 MB/s** | Full ACID Insert (WAL + Indexing) |
| **Rand Update** | **690,000 Ops/s** | **~5 MB/s** | Atomic Cycle: Index Lookup → Bitmap Mark → WAL → RAM Insert |
| **Seq Scan (Dirty)** | **92.8 Million Rows/s** | **707 MB/s** | Correctness Path (Scanning with Deletion Bitmaps) |

> **Note on "Dirty" Scans:** When you update rows, blocks become "dirty". AwanDB automatically switches to a slower, safer scan path to ensure Snapshot Isolation. The Background Compactor (Phase 7) will periodically clean these blocks to restore AVX speed.

## 💻 Usage Example: Relational HTAP

AwanDB allows you to define strict schemas. The engine ensures that when you insert a "row", the columnar stores for Integers, Strings, and Floats remain perfectly aligned.

```scala
import org.awandb.core.engine.AwanTable
import org.awandb.core.query._

// 1. Define Schema
// The engine manages these separate columns as a single coherent table.
val users = new AwanTable("users", capacity = 100000, dataDir = "./data/users")
users.addColumn("uid")  // Int Column
users.addColumn("age")  // Int Column

// 2. OLTP: High-Speed Ingestion
// Transactions are written to WAL and MemTable instantly.
users.insertRow(Array(1, 25))
users.insertRow(Array(2, 30))

// 3. ACID Updates
// Updates are atomic. Readers will never see a "half-updated" state.
// This performs a Delete (Bitmap) + Insert (Append) internally.
val changes = Map("age" -> 31)
users.update(id = 2, changes)

// 4. Persistence (Optional manual flush, usually handled by background thread)
users.flush()

// 5. OLAP: Analytical Query
// Query: "Count users where age > 20"
val count = users.query(threshold = 20)
println(s"Users over 20: $count")

users.close()

```

## ⚙️ Build Instructions

**1. Build Native Engine (C++)**

```bash
cd src/main/cpp
mkdir build && cd build
cmake -G "Visual Studio 17 2022" -A x64 ..  # Or `cmake ..` on Linux
cmake --build . --config Release

```

*Copy the resulting library to `lib/Release/`.*

**2. Run Tests (Scala)**

```bash
sbt test

```

## 📄 License

Copyright (c) 2026 Iskandar & Contributors.
This project is licensed under the Apache 2.0 License.