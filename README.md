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
    * **OLTP:** Async, actor-model ingestion (Netty-style) handles massive concurrency and burst writes via Write Fusion.
    * **OLAP:** Vectorized C++ kernels scan data at memory bandwidth limits (>30 GB/s).
* **Columnar-First, Integer-Optimized:** We are an "Integer-First" database. We prioritize financial and scientific data types for maximum CPU throughput, while efficiently handling Strings via Dictionary Encoding.
* **Relational Schema Enforcement:** Define tables by combining columns. The engine automatically enforces row alignment and integrity, supporting **1:1 and 1:N relationships**.
* **ANSI-SQL Style Analytics:** Full support for **Hash Joins**, **Late Materialization**, and **Vectorized Aggregations**.
* **Vector Native:** Built-in support for **Vector Embeddings** (128-dim) with Cosine Similarity, enabling AI workloads alongside relational data.

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
| **2** | Storage | **Block Manager** | ✅ DONE | Atomic snapshot isolation. |
| **3** | Arch | **Manager Thread** | ✅ DONE | Concurrency management. |
| **3** | Compute | **Shared Scan** | ✅ DONE | Massive throughput scaling for concurrent queries. |
| **3** | Query | **Zone Map Skipping** | ✅ DONE | Statistical pruning of data blocks. |
| **3** | Transact | **Write Fusion** | ✅ DONE | High-throughput batch ingestion. |
| **3** | Index | **Cuckoo Filters** | ✅ DONE | Constant-time point lookups. |
| **3** | Compute | **Aggressive Unrolling** | ✅ DONE | Maximizing memory bandwidth utilization. |

### **Phase 4: The Type System (Completed)**
*Handling Complex Data (Strings, Vectors) without losing Integer speed.*

| Phase | Module | Feature | Status | Impact |
| :--- | :--- | :--- | :--- | :--- |
| **4** | Types | **German String Layout** | ✅ DONE | High-performance text filtering. |
| **4** | Types | **Vector Embeddings** | ✅ DONE | Native support for AI/RAG workloads. |
| **4** | Compute | **Vector Hashing** | ✅ DONE | Analytical primitives for complex types. |

### **Phase 5: The Query Execution Engine (Completed)**
*Transforming from "Fast Scan" to "Complex Analytics" (Joins, Aggs, Sorting).*

| Phase | Module | Feature | Status | Impact |
| :--- | :--- | :--- | :--- | :--- |
| **5** | Arch | **Morsel-Driven Parallelism** | ✅ DONE | Dynamic load balancing across cores. |
| **5** | Compute | **Radix Sort** | ✅ DONE | 3x Faster Sorting vs Java Parallel Sort. |
| **5** | Compute | **Vectorized Hash Agg** | ✅ DONE | 7x Faster Grouping vs Java HashMap. |
| **5** | Compute | **Shared Filter (SIP)** | ✅ DONE | 2x Faster Joins via Cuckoo pushdown. |
| **5** | Pipeline | **Vectorized DAG** | ✅ DONE | Zero-copy batch processing pipeline. |
| **5** | Types | **Dictionary Encoding** | ✅ DONE | **35x Faster** Sorting for Strings. |
| **5** | Query | **Hash Join Operator** | ✅ DONE | **1.5x Faster** Multi-Table Joins (SIP). |
| **5** | Query | **Late Materialization** | ✅ DONE | Efficient execution on Wide Tables. |
| **5** | Query | **Operator DAG Scheduler** | ✅ DONE | Execution planning for complex queries. |
| **5** | Storage | **Bit-Packing / RLE** | ✅ DONE | Integer compression to reduce memory footprint. |

### **Phase 6: The Platform (Current Focus)**
*Networking, Graph, and Hardware Awareness.*

| Phase | Module | Feature | Status | Impact |
| :--- | :--- | :--- | :--- | :--- |
| **6** | Ingest | **JSON Shredder** | 📅 New | High-performance semi-structured data ingestion. |
| **6** | Network | **Arrow Flight (Tier C)** | 📅 Pending | Standard network interface for clients. |
| **6** | Graph | **Adjacency Index** | 📅 New | Optimized storage for graph relationships. |
| **6** | Graph | **Recursive Traversal** | 📅 New | Native support for graph traversal queries. |

## 📊 Performance Metrics (Verified)

*Benchmarks run on Ryzen 9 5900X, DDR4 RAM (Single Channel).*

### ⚡ OLTP (Transactional) Performance
| Workload | Throughput | Notes |
| :--- | :--- | :--- |
| **Seq Write (Direct)** | **118 Million Rows/s** | Fused Batch Insert |
| **Async Write (API)** | **~5 Million Ops/s** | Full ACID path via EngineManager |
| **Concurrency** | **Lock-Free Read/Write** | Readers unaffected by heavy writes |

### 🔍 OLAP (Analytical) Performance
| Workload | Throughput | Speedup vs Standard |
| :--- | :--- | :--- |
| **Seq Scan (Memory)** | **8.6 Billion Rows/s** | AVX-512 Unrolled |
| **Aggregation** | **22 Million Groups/s** | **7.44x** vs HashMap |
| **Radix Sort** | **211 Million Items/s** | **4x** vs Java Parallel Sort |
| **Hash Join** | **25 Million Joins/s** | SIP Optimized (50M rows < 2s) |
| **Vector Search** | **14.8 GB/s** | Cosine Similarity (128-dim) |
| **Query Fusion** | **13,040 Q/s** | **16x** vs Standard Scan |

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

val orders = new AwanTable("orders", capacity = 100000, dataDir = "./data/orders")
orders.addColumn("oid")     // Int
orders.addColumn("uid_fk")  // Int (Foreign Key)
orders.addColumn("amount")  // Long
orders.addColumn("city")    // String (Dictionary Encoded)

// 2. OLTP: High-Speed Ingestion
// Transactions are written to WAL and MemTable instantly.
users.insertRow(Array(1, 25))
users.insertRow(Array(2, 30))

orders.insertRow(Array(100, 1, 500, "Kuala Lumpur"))
orders.insertRow(Array(101, 1, 300, "Penang"))
orders.insertRow(Array(102, 2, 900, "Johor Bahru"))

// 3. Persistence (Optional manual flush, usually handled by background thread)
users.flush()
orders.flush()

// 4. OLAP: Complex Analytical Query
// Query: "Sum of Amount by City where User.Age > 20"
// This involves: Scan -> Filter -> Hash Join -> Materialize String -> Group By
val usersScan = new TableScanOperator(users.blockManager, keyCol=0, valCol=1)
val buildOp   = new HashJoinBuildOperator(new HashAggOperator(usersScan))

val ordersScan = new TableScanOperator(orders.blockManager, keyCol=1, valCol=2)
val joinOp     = new HashJoinProbeOperator(ordersScan, buildOp)
val resultOp   = new MaterializeOperator(joinOp, colIdx=3) // Fetch 'City' late

resultOp.open()
var batch = resultOp.next()
while (batch != null) {
  println(s"Result Batch: ${batch.count} rows joined & analyzed.")
  batch = resultOp.next()
}
resultOp.close()

```

## ⚙️ Build Instructions

**1. Build Native Engine (C++)**

```bash
cd src/main/resources/native
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