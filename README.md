# ☁️ AwanDB Core (OSS)

**A High-Performance, Hybrid Columnar Database Engine.**

AwanDB Core is the open-source storage and compute engine powering the AwanDB platform. It combines the safety and concurrency of **Scala** with the raw throughput of **C++ AVX-512 Intrinsics**.

## 🚀 Key Features

* **Hybrid Architecture:** Scala Control Plane (Netty/Akka style async loop) + C++ Data Plane (JNI).
* **SIMD-Accelerated Scans:** Uses AVX2/AVX-512 instructions to scan data at memory bandwidth speeds (>20 GB/s).
* **Query Fusion (Shared Scans):** Automatically fuses multiple concurrent queries into a single scan pass, allowing query throughput to scale *with* load.
* **Unified Memory:** Custom memory allocator that aligns data on 64-byte boundaries for zero-copy access between Java and C++.
* **Pluggable Governance:** Hooks for rate limiting and tenancy (used by the Enterprise Edition).

## 🛠️ Architecture

AwanDB uses a **Single-Writer, Multi-Reader** architecture managed by an asynchronous `EngineManager`.

```mermaid
graph TD
    User[User API] -->|Async Submit| EM[Engine Manager (Scala)]
    EM -->|Batch Write| WAL[Write Ahead Log]
    EM -->|Insert| RAM[MemTable (Off-Heap)]
    EM -->|Flush| Disk[Columnar Blocks (.udb)]
    
    subgraph "Native Compute Layer (C++)"
    RAM -.->|JNI Pointer| AVX[AVX-512 Kernels]
    Disk -.->|mmap| AVX
    end

```

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
cd awandb-core/src/main/cpp

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

* **From:** `awandb-core/src/main/cpp/build/Release/`
* **To:** `awandb-core/lib/Release/`

*(Note: If using VS Code, the provided `tasks.json` handles this automatically via the "Copy DLL to Core" task).*

### 2. Run Tests (Scala)

Once the DLL is in place, run the full suite:

```bash
cd awandb-core
sbt test

```

You should see output indicating the Native Engine loaded successfully:

```text
[NativeBridge] [SUCCESS] Loaded High-Performance C++ Engine.
[EngineManager] Started Event Loop
...
[info] All tests passed.

```

## 💻 Usage Example

AwanDB Core provides a low-level `AwanTable` API for building custom data platforms.

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
val countFuture = table.engineManager.submitQuery(24)

countFuture.foreach { result =>
  println(s"Sensors above 24°C: $result") // Output: 2
}

// 4. Persistence
table.engineManager.submitFlush() // Writes immutable block to disk
table.close()

```

## 📊 Performance Benchmarks

*Hardware: Ryzen 9 5900X, DDR4 RAM.*

| Workload | Throughput | Notes |
| --- | --- | --- |
| **Ingest (WAL + RAM)** | ~8.5 Million Ops/sec | Single-threaded writer |
| **Scan (Warm Cache)** | ~320 Million Rows/sec | AVX-512 Filter |
| **Shared Scan (100 Qs)** | 0.9x Latency Penalty | Effectively free concurrency |

## 📂 Project Structure

* `src/main/scala`: The Database Management System (DBMS) logic.
* `engine/`: `EngineManager`, `AwanTable`, Governance hooks.
* `storage/`: `BlockManager`, `Wal`, `NativeColumn`.
* `jni/`: `NativeBridge` (The JNI connector).


* `src/main/cpp`: The raw compute engine.
* `engine.cpp`: JNI implementation and AVX kernels.
* `block.h`: Memory layout definitions.



## 📄 License

Copyright (c) 2026 Iskandar & Contributors.
This project is licensed under the Apache 2.0 License.