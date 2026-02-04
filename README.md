# ☁️ AwanDB Core (OSS)

**A High-Performance, Hybrid Columnar Database Engine.**

AwanDB Core is the open-source storage and compute engine powering the AwanDB platform. 
It combines the safety and concurrency of **Scala** with the raw throughput of **C++ AVX-512 Intrinsics**. 

> **Project Goal:** AwanDB is designed to evolve from a high-speed storage engine into a **full-fledged, standalone analytical database**. While currently available as an embedded core, the roadmap targets a complete server architecture with network interfaces (Arrow Flight) and distributed capabilities.

## 🚀 Key Features

* **Hybrid Architecture:** Scala Control Plane (Netty/Akka style async loop) + C++ Data Plane (JNI).
* **Multi-Model Support:** Native storage for Integers, Floats, **German Strings**, and **Vector Embeddings**.
* **SIMD-Accelerated Scans:** Uses AVX2/AVX-512 instructions to scan data at memory bandwidth speeds (>40 GB/s L3).
* **Query Fusion (Shared Scans):** Automatically fuses multiple concurrent queries into a single scan pass.
* **Zero-Copy Memory:** Custom allocator aligning data on 64-byte boundaries for direct JNI access.
* **Morsel-Driven Parallelism:** (Upcoming) Dynamic task scheduling for perfect core utilization.

## 🗺️ OSS Roadmap (v2026.02)

We are transitioning AwanDB from a "Fast Storage Engine" to a "High-Performance Analytical Database." The current focus is on the **Type System (Phase 4)** and the **Query Execution Engine (Phase 5)**.

### **Phase 0–3: The Core Engine (Completed)**
*Foundation, Storage, and Raw Compute Speed.*

| Phase | Priority | Module | Feature | License | Status | Why / Commercial Impact |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **0** | P0 | Memory | **Aligned Allocator** | OSS | ✅ DONE | Foundation for Zero-Copy Wasm & AVX alignment. |
| **0** | P0 | Storage | **Arrow Block Layout** | OSS | ✅ DONE | Standard format allows "Zero-Serde" network transfer. |
| **1** | P1 | Transact | **WAL (Durability)** | OSS | ✅ DONE | Required for SLA guarantees. |
| **1** | P1 | Safety | **Overflow Guard** | ENT | ✅ DONE | Prevents one tenant from crashing the node. |
| **2** | P1 | Storage | **Block I/O (LSM)** | OSS | ✅ DONE | Data persists to immutable blocks on disk. |
| **2** | P1 | Storage | **Block Manager** | OSS | ✅ DONE | Tracks loaded blocks (Atomic Snapshots). |
| **3** | P1 | Arch | **Manager Thread** | OSS | ✅ DONE | Central point for concurrency. |
| **3** | P1 | Compute | **Shared Scan** | OSS | ✅ DONE | **23x Speedup**. One AVX pass answers 100+ concurrent queries. |
| **3** | P1 | Query | **Zone Map Skipping** | OSS | ✅ DONE | Predicate Pushdown. C++ skips blocks via Header check. |
| **3** | P1 | Transact | **Write Fusion** | OSS | ✅ DONE | **5x Speedup**. Single Lock/Syscall for batch inserts. |
| **3** | P2 | Index | **Cuckoo Filters** | OSS | ✅ DONE | Point Lookup. O(1) check if ID exists. |
| **3** | P2 | Compute | **Aggressive Unrolling** | OSS | ✅ DONE | **Memory Bandwidth Saturation**. 8x AVX unroll fills RAM bus. |

### **Phase 4: The Type System (Current Focus)**
*Handling Complex Data (Strings, Vectors) without losing Integer speed.*

| Phase | Priority | Module | Feature | License | Status | Why / Commercial Impact |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **4** | P2 | Types | **German String Layout** | OSS | 🚧 In Prog | **"Zuckerberg" Search**. Inline 4-byte prefix for AVX filtering. |
| **4** | P2 | Types | **Vector Embeddings** | OSS | 📅 New | **AI Native**. Vecf32 support for RAG / Similarity Search. |
| **4** | P3 | Compute | **Vector Hashing** | OSS | 📅 New | Analytics. Prerequisite for GROUP BY on non-int types. |
| **4** | P3 | Ops | **Usage Metering** | ENT | 📅 Pending | Billing feed for Governor (Row scans/sec). |
| **4** | P3 | Security | **Data Masking** | ENT | 📅 New | Compliance (PII Redaction at storage level). |

### **Phase 5: The Query Execution Engine (New)**
*Transforming from "Fast Scan" to "Complex Analytics" (Joins, Aggs, Sorting).*

| Phase | Priority | Module | Feature | License | Status | Why / Commercial Impact |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **5** | **P1** | Arch | **Morsel-Driven Parallelism**| OSS | 📅 **New** | **Load Balancing**. Threads pull small tasks ("morsels") from a shared pool. |
| **5** | **P1** | Query | **Operator DAG Scheduler** | OSS | 📅 **New** | **Complex Queries**. Dependency graph for Join Build/Probe phases. |
| **5** | **P1** | Compute | **Shared Filter (SIP)** | OSS | 📅 **New** | **Fast Joins**. Pass Cuckoo Filters from Build side to Probe scanner. |
| **5** | **P2** | Compute | **Vectorized Hash Agg** | OSS | 📅 **New** | **GROUP BY**. High-speed hash map for integer aggregation. |
| **5** | **P2** | Compute | **Radix Sort** | OSS | 📅 **New** | **ORDER BY**. O(N) sorting for integers (much faster than std::sort). |
| **5** | P4 | Types | **Dictionary Encoding** | OSS | 📅 **New** | **Compression**. Turns Strings into Ints for 4x faster scans/grouping. |
| **5** | P4 | Storage | **Bit-Packing / RLE** | OSS | 📅 **New** | Integer Compression. Reduces RAM usage by 50-80%. |
| **5** | P4 | Query | **Late Materialization** | OSS | 📅 **New** | Efficiency. Only fetch Strings/Blobs at the very end of query. |

### **Phase 6: The Platform (Scale & Distribution)**
*Networking, Graph, and Hardware Awareness.*

| Phase | Priority | Module | Feature | License | Status | Why / Commercial Impact |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **6** | P3 | Ingest | **JSON Shredder** | OSS | 📅 New | Usability. SIMD JSON parser to flatten data on ingest. |
| **6** | P3 | Network | **Arrow Flight (Tier C)** | OSS | 📅 Pending | **Standard User**. Network Bridge for Cheap Client CPUs. |
| **6** | P4 | Graph | **Adjacency Index** | OSS | 📅 New | Graph Native. Optimized Edge storage for fast traversal. |
| **6** | P4 | Graph | **Recursive Traversal** | OSS | 📅 New | **Graph Queries**. BFS / Recursive CTEs. |
| **6** | P5 | Compute | **Wasm Runtime (Tier B)** | ENT | 📅 Pending | Safe User. Sandboxed SQL Logic (UDFs). |
| **6** | P5 | Compute | **DMA Bridge (Tier A)** | ENT | 📅 New | **Power User**. Zero-Copy Pointer Handoff (GPU Direct). |
| **6** | P5 | Arch | **Topology Discovery** | ENT | 📅 New | Hardware Aware (NUMA / Core pinning). |
| **6** | P5 | Storage | **Disk Sharding** | ENT | 📅 New | IO Locality. Striping data across multiple NVMe drives. |
| **6** | P6 | Hardware | **Unified Compute** | ENT | 📅 New | **AI Scale**. Porting AVX kernels to GPU (HIP/SYCL). |

## 🛠️ Architecture

AwanDB uses a **Single-Writer, Multi-Reader** architecture managed by an asynchronous `EngineManager`.

1.  **User API / Network Layer:** Submits asynchronous requests (Insert/Query) to the **Engine Manager (Scala)**.
2.  **Engine Manager:**
    * Batches writes into the **Write Ahead Log (WAL)** for durability.
    * Inserts data into the **Off-Heap MemTable (RAM)**.
3.  **Persistence:** Periodically flushes RAM buffers to immutable **Columnar Blocks (.udb)** on disk.
4.  **Native Compute Layer (C++):**
    * Accesses RAM via direct JNI Pointers (Zero-Copy).
    * Accesses Disk via Memory Mapping (mmap).
    * Executes hyper-optimized **AVX-512 Kernels** for filtering and aggregation.

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
| **Scan (L3 Cache)** | **~11.5 Billion Rows/sec** | ~43 GB/s | AVX-512 (8x Unroll) |
| **Scan (Main RAM)** | **~4.6 Billion Rows/sec** | ~17.6 GB/s | RAM Bandwidth Limited |
| **Shared Scan** | **23x Speedup** | N/A | 100 Queries in 1 Pass |

## 📂 Project Structure

* `src/main/scala`: The Database Management System (DBMS) logic.
* `engine/`: `EngineManager`, `AwanTable`, Governance hooks.
* `storage/`: `BlockManager`, `Wal`, `NativeColumn`.
* `jni/`: `NativeBridge` (The JNI connector).


* `src/main/resources/native`: The raw compute engine.
* `engine.cpp`: JNI implementation and AVX kernels.
* `block.h`: Memory layout definitions.



## 📄 License

Copyright (c) 2026 Iskandar & Contributors.
This project is licensed under the Apache 2.0 License.
