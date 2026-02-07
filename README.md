# ☁️ AwanDB Core (OSS)

**A High-Performance, Hybrid Columnar Database Engine.**

AwanDB Core is the open-source storage and compute engine powering the AwanDB platform. It combines the safety and concurrency of **Scala** with the raw throughput of **C++ AVX-512 Intrinsics**.

> **Project Goal:** AwanDB is designed to evolve from a high-speed storage engine into a **full-fledged, standalone analytical database**. While currently available as an embedded core, the roadmap targets a complete server architecture with network interfaces (Arrow Flight) and distributed capabilities.

## 🚀 Key Features

* **Hybrid Architecture:** Scala Control Plane (Netty/Akka style async loop) + C++ Data Plane (JNI).
* **Multi-Model Support:** Native storage for Integers, Floats, **German Strings**, and **Vector Embeddings**.
* **SIMD-Accelerated Scans:** Uses AVX2/AVX-512 instructions to scan data at memory bandwidth speeds (>52 GB/s L3).
* **Vectorized Execution Pipeline:** Processes data in cache-resident batches (Volcano Model), minimizing JNI overhead.
* **Query Fusion (Shared Scans):** Automatically fuses multiple concurrent queries into a single scan pass.
* **Zero-Copy Memory:** Custom allocator aligning data on 64-byte boundaries for direct JNI access.
* **Morsel-Driven Parallelism:** Dynamic task scheduling for perfect core utilization.

## 🗺️ OSS Roadmap (v2026.02)

We are transitioning AwanDB from a "Fast Storage Engine" to a "High-Performance Analytical Database."

### **Phase 0–3: The Core Engine (Completed)**

*Foundation, Storage, and Raw Compute Speed.*

| Phase | Module | Feature | Status | Impact |
| --- | --- | --- | --- | --- |
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
| --- | --- | --- | --- | --- |
| **4** | Types | **German String Layout** | ✅ DONE | High-performance text filtering. |
| **4** | Types | **Vector Embeddings** | ✅ DONE | Native support for AI/RAG workloads. |
| **4** | Compute | **Vector Hashing** | ✅ DONE | Analytical primitives for complex types. |

### **Phase 5: The Query Execution Engine (Current Focus)**

*Transforming from "Fast Scan" to "Complex Analytics" (Joins, Aggs, Sorting).*

| Phase | Module | Feature | Status | Impact |
| --- | --- | --- | --- | --- |
| **5** | Arch | **Morsel-Driven Parallelism** | ✅ DONE | Dynamic load balancing across cores. |
| **5** | Compute | **Radix Sort** | ✅ DONE | 3x Faster Sorting vs Java Parallel Sort. |
| **5** | Compute | **Vectorized Hash Agg** | ✅ DONE | 7x Faster Grouping vs Java HashMap. |
| **5** | Compute | **Shared Filter (SIP)** | ✅ DONE | 2x Faster Joins via Cuckoo pushdown. |
| **5** | Pipeline | **Vectorized DAG** | ✅ DONE | Zero-copy batch processing pipeline. |
| **5** | Types | **Dictionary Encoding** | 📅 **Next** | Compression for repetitive string data. |
| **5** | Query | **Operator DAG Scheduler** | 📅 Pending | Execution planning for complex queries. |
| **5** | Storage | **Bit-Packing / RLE** | 📅 Pending | Integer compression to reduce memory footprint. |
| **5** | Query | **Late Materialization** | 📅 Pending | Improving query efficiency by deferring data fetches. |

### **Phase 6: The Platform (Scale & Distribution)**

*Networking, Graph, and Hardware Awareness.*

| Phase | Module | Feature | Status | Impact |
| --- | --- | --- | --- | --- |
| **6** | Ingest | **JSON Shredder** | 📅 New | High-performance semi-structured data ingestion. |
| **6** | Network | **Arrow Flight (Tier C)** | 📅 Pending | Standard network interface for clients. |
| **6** | Graph | **Adjacency Index** | 📅 New | Optimized storage for graph relationships. |
| **6** | Graph | **Recursive Traversal** | 📅 New | Native support for graph traversal queries. |

## 🛠️ Architecture

AwanDB uses a **Single-Writer, Multi-Reader** architecture managed by an asynchronous `EngineManager`.

1.  **User API:** Submits asynchronous requests (Insert/Query) to the **Engine Manager (Scala)**.
2.  **Engine Manager:**
    * Batches writes into the **Write Ahead Log (WAL)** for durability.
    * Inserts data into the **Off-Heap MemTable (RAM)**.
3.  **Persistence:** Periodically flushes RAM buffers to immutable **Columnar Blocks (.udb)** on disk.
4.  **Native Compute Layer (C++):**
    * Accesses RAM via direct JNI Pointers (Zero-Copy).
    * Accesses Disk via Memory Mapping (mmap).
    * Executes hyper-optimized **AVX-512 Kernels** for filtering, sorting, and aggregation.

## 📦 Prerequisites

* **JDK:** Java 11 or 21+ (Tested on Adoptium OpenJDK).
* **Scala:** 2.13 or 3.3.
* **Build Tool:** `sbt`.
* **C++ Compiler:**
    * **Windows:** Visual Studio 2022 (MSVC).
    * **Linux/Mac:** GCC 9+ or Clang (with AVX2 support).
* **CMake:** 3.10+.

## ⚙️ Build Instructions

This is a hybrid project. You must build the C++ native engine before running the Scala code.

### 1. Build the Native Engine (C++)

```bash
# Navigate to the C++ source
cd awandb-core/src/main/resources/native

# Create build directory
mkdir build && cd build

# Configure (Windows)
cmake -G "Visual Studio 17 2022" -A x64 ..
# Configure (Linux/Mac)
# cmake ..

# Build (Release mode is critical for AVX speed)
cmake --build . --config Release

```

**Post-Build Step:**
Copy the generated shared library (`awan_engine_core.dll` or `libawan_engine_core.so`) to the Scala library path:

* **From:** `awandb-core/src/main/resources/native/build/Release/`
* **To:** `awandb-core/lib/Release/`

*(Note: If using VS Code, the provided `tasks.json` handles this automatically via the "Copy DLL to Core" task).*

### 2. Run Tests (Scala)

Once the DLL is in place, run the full suite:

```bash
cd awandb-core
sbt test

```

## 💻 Usage Example (Core API)

*Note: This demonstrates the low-level Core API. A network server (Arrow Flight) interface is planned for Phase 6.*

```scala
import org.awandb.core.engine.AwanTable

// 1. Initialize Table (Stored in ./data)
val table = new AwanTable("iot_sensors", capacity = 1_000_000, dataDir = "./data")
table.addColumn("temperature")

// 2. High-Speed Ingestion (Async)
// The EngineManager batches these automatically.
table.engineManager.submitInsert(25)
table.engineManager.submitInsert(30)
table.engineManager.submitInsert(22)

// 3. Query (Counts items > threshold)
// Returns Future[Int]
val countFuture = table.engineManager.submitQuery("temperature", 24)

countFuture.foreach { result =>
  println(s"Sensors above 24°C: $result") // Output: 2
}

// 4. Persistence
table.engineManager.submitFlush() // Writes immutable block to disk
table.close()

```

## 📊 Performance Benchmarks

*Hardware: Ryzen 9 5900X, DDR4 RAM (Single Channel).*

| Workload | Throughput | Bandwidth | Notes |
| --- | --- | --- | --- |
| **Seq Write (WAL + RAM)** | **~70 Million Ops/sec** | ~270 MB/s | Batch Fused |
| **Scan (L3 Cache)** | **~11.5 Billion Rows/sec** | ~52 GB/s | AVX-512 (8x Unroll) |
| **Scan (Main RAM)** | **~4.6 Billion Rows/sec** | ~17.6 GB/s | RAM Bandwidth Limited |
| **Aggregation (SUM)** | **~22 Million Rows/sec** | N/A | 7x faster than HashMap |
| **Sorting (Radix)** | **~50 Million Rows in 180ms** | N/A | 3x faster than Java Sort |
| **Shared Scan** | **23x Speedup** | N/A | 100 Queries in 1 Pass |

## 📂 Project Structure

* `src/main/scala`: The Database Management System (DBMS) logic.
* `engine/`: `EngineManager`, `AwanTable`, Governance hooks.
* `storage/`: `BlockManager`, `Wal`, `NativeColumn`.
* `query/`: `Operator`, `VectorBatch` (The Execution Engine).
* `jni/`: `NativeBridge` (The JNI connector).
* `src/main/resources/native`: The raw compute engine.
* `engine.cpp`: Removed (Modularized).
* `compute.cpp`: Scan & Filter Kernels.
* `sort.cpp`: Parallel Radix Sort.
* `aggregation.cpp`: Hash Aggregation.
* `cuckoo_jni.cpp`: Filter Logic.
* `common.cpp`: Memory & JNI Helpers.



## 📄 License

Copyright (c) 2026 Iskandar & Contributors.
This project is licensed under the Apache 2.0 License.