```markdown
# ☁️ AwanDB Core (OSS)

**A High-Performance, Hybrid Columnar Database Engine.**

AwanDB Core is the open-source storage and compute engine powering the AwanDB platform. It combines the safety and concurrency of **Scala** with the raw throughput of **C++ AVX-512 Intrinsics** to deliver analytics and vector search at memory-bandwidth speeds.

## 🚀 Key Features

* **Hybrid Architecture:** Scala Control Plane (Netty/Akka style async loop) + C++ Data Plane (JNI).
* **Multi-Model Storage:**
    * **Integers:** Bit-packed, SIMD-scanned at >40 GB/s.
    * **German Strings:** 16-byte fixed layout with inline prefix filtering (~31 GB/s).
    * **Vector Embeddings:** Native `Vecf32` support with AVX2 FMA Cosine Similarity (~17 GB/s).
* **Query Fusion (Shared Scans):** Automatically fuses multiple concurrent queries into a single scan pass, allowing query throughput to scale *with* load.
* **Unified Memory:** Custom memory allocator that aligns data on 64-byte boundaries for zero-copy access between Java and C++.
* **Pluggable Governance:** Hooks for rate limiting and tenancy.

## 🗺️ OSS Roadmap

We are building AwanDB in strictly prioritized phases. We are currently transitioning from **Phase 4** to **Phase 5**.

| Phase | Module | Feature | Status | Impact |
| :--- | :--- | :--- | :--- | :--- |
| **0–3** | **Core Engine** | **AVX-512 Integers** | ✅ **DONE** | Foundation. 43 GB/s Int Scans. |
| **0–3** | **Storage** | **Block I/O & WAL** | ✅ **DONE** | Persistence and Durability. |
| **0–3** | **Compute** | **Shared Scans** | ✅ **DONE** | 11x Speedup on concurrent loads. |
| **4** | **Types** | **German Strings** | ✅ **DONE** | "Zuckerberg" Search. Text filtering at int speed. |
| **4** | **Types** | **Vector Embeddings** | ✅ **DONE** | AI-Native. Zero-Cost Cosine Similarity. |
| **4** | **Compute** | **Hardware Hashing** | ✅ **DONE** | CRC32 primitives for Aggregations. |
| **5** | **Arch** | **Morsel Parallelism** | 🚧 **In Progress** | Load Balancing. Threads pull small tasks from pool. |
| **5** | **Query** | **Operator DAG** | 📅 Planned | Execution Graph (Scan -> Filter -> Join). |
| **5** | **Compute** | **Vectorized Hash Agg** | 📅 Planned | High-speed `GROUP BY`. |
| **5** | **Compute** | **Radix Sort** | 📅 Planned | O(N) Integer Sorting. |
| **6** | **Network** | **Arrow Flight** | 📅 Planned | Zero-Serde network transport. |
| **6** | **Hardware** | **Unified Compute** | 📅 Planned | GPU Offload (HIP/SYCL). |

## 🛠️ Architecture

AwanDB uses a **Single-Writer, Multi-Reader** architecture managed by an asynchronous `EngineManager`.

1.  **User API:** Submits asynchronous requests (Insert/Query) to the **Engine Manager (Scala)**.
2.  **Engine Manager:**
    * Batches writes into the **Write Ahead Log (WAL)** for durability.
    * Inserts data into the **Off-Heap MemTable (RAM)**.
3.  **Persistence:** Periodically flushes RAM buffers to immutable **Columnar Blocks (.udb)** on disk.
4.  **Native Compute Layer (C++):**
    * **Zero-Copy:** Accesses RAM via direct JNI Pointers.
    * **Memory Mapping:** Accesses Disk via `mmap`.
    * **Kernels:** Executes hyper-optimized AVX-512/AVX2 kernels for Filtering, Similarity, and Aggregation.

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

## 💻 Usage Example

AwanDB Core provides a low-level `NativeBridge` API for building custom data engines.

```scala
import org.awandb.core.jni.NativeBridge

// 1. Setup: Allocate a Block for 1 Million Vectors (128-dim)
val rows = 1_000_000
val dim = 128
val blockPtr = NativeBridge.createBlock(rows * dim, 1)

// 2. Ingest: Load Normalized Vector Data (Zero-Copy)
// 'data' is a standard Heap Float Array
NativeBridge.loadVectorData(blockPtr, 0, dataArray, dim)

// 3. Query: Cosine Similarity Search
// Find rows with similarity > 0.95
val count = NativeBridge.avxScanVectorCosine(blockPtr, 0, queryVector, 0.95f)

println(s"Found $count matches in ${rows/1e6}M vectors.")

// 4. Cleanup
NativeBridge.freeMainStore(blockPtr)

```

## 📊 Performance Benchmarks

*Hardware: Ryzen 9 5900X, DDR4 RAM (Single Channel).*

| Workload | Type | Throughput | Bandwidth | Notes |
| --- | --- | --- | --- | --- |
| **Integer Scan** | `int32` | **~11.5 Billion Rows/s** | **43 GB/s** | AVX-512 (L3 Cache) |
| **String Scan** | `GermanStr` | **~1.0 Billion Rows/s** | **31 GB/s** | Prefix Filter Optimization |
| **Vector Search** | `Vecf32` | **~18 Million Rows/s** | **17.7 GB/s** | Cosine Similarity (128-dim) |
| **Shared Scan** | `int32` | **23x Speedup** | N/A | 100 Concurrent Queries |

## 📂 Project Structure

* `src/main/scala`: The Database Management System (DBMS) logic.
* `engine/`: `EngineManager`, `AwanTable`, Governance hooks.
* `storage/`: `BlockManager`, `Wal`, `NativeColumn`.
* `jni/`: `NativeBridge` (The JNI connector).


* `src/main/resources/native`: The raw compute engine.
* `engine.cpp`: JNI implementation and AVX kernels.
* `block.h`: Memory layout definitions.
* `german_string.h`: 16-byte String Layout.
* `cuckoo.h`: Fast Set Membership.



## 📄 License

Copyright (c) 2026 Iskandar & Contributors.
This project is licensed under the Apache 2.0 License.

```

```