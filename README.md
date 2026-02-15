# ☁️ AwanDB Core (OSS)

**The Embedded & Standalone HTAP Database: Postgres Concurrency meets DuckDB Speed.**

AwanDB Core is an open-source, **Hybrid Transactional/Analytical Processing (HTAP)** engine. It is designed to handle high-velocity data ingestion (OLTP) and complex, blazing-fast reporting (OLAP) within a single runtime.

> **Project Vision:** Why run two databases?
> Typically, developers write to a transactional DB (like Postgres) and sync data to an analytical DB (like DuckDB/ClickHouse) for reporting.
> **AwanDB unifies this.** It accepts ACID transactions at millions of ops/sec and executes analytical queries on that same data microseconds later, using raw C++ AVX-512 intrinsics.

## 🚀 Key Features

* **Deployment Flexibility:** Run it deeply embedded within your JVM application for zero-latency data access, or run it as a standalone server over the network.
* **Full SQL Execution Pipeline:** Built-in ANSI SQL parser mapping directly to a Volcano execution model. Supports compound predicates (`AND`/`OR`), `JOIN`, `GROUP BY`, `ORDER BY`, and `LIMIT` with late materialization.
* **True HTAP Architecture:**
* **OLTP:** Async, actor-model ingestion handles massive concurrency.
* **OLAP:** Vectorized C++ kernels scan data at memory bandwidth limits (>100 GB/s).


* **Hardware-Accelerated Predicate Pushdown:** AST evaluations (like `WHERE score > 100`) are pushed down into native C++ bitmasks using `_BitScanForward` and AVX-512 intrinsics.
* **Hybrid Scan Strategy:**
* **Fast Path:** Blocks with no deletions are scanned via raw AVX-512 instructions (Clean Scan).
* **Safe Path:** Blocks with active deletions use a bitmap filter to ensure Snapshot Isolation (Dirty Scan).


* **Columnar-First, Integer-Optimized:** Prioritizes financial and scientific data types for maximum CPU throughput, while efficiently handling Strings via Dictionary Encoding.

## 🗺️ OSS Roadmap (v2026.02)

We have evolved from a storage engine into a complete HTAP system.

### **Phase 0–5: Storage & Compute (Completed)**

*Foundation, Storage, and Raw Compute Speed.*

| Phase | Module | Feature | Status | Impact |
| --- | --- | --- | --- | --- |
| **0** | Memory | **Aligned Allocator** | ✅ DONE | Foundation for Zero-Copy operations. |
| **1** | Transact | **WAL (Durability)** | ✅ DONE | Data durability guarantees. |
| **2** | Storage | **Block I/O (LSM)** | ✅ DONE | Immutable disk persistence. |
| **3** | Compute | **Shared Scan** | ✅ DONE | Massive throughput scaling for concurrent queries. |
| **4** | Types | **German String Layout** | ✅ DONE | High-performance text filtering. |
| **5** | Compute | **Hybrid Scan** | ✅ DONE | Adaptive scanning for Clean vs. Dirty blocks. |

### **Phase 6: The SQL API (Completed)**

*Parsing, Planning, and the Volcano Execution Model.*

| Phase | Module | Feature | Status | Impact |
| --- | --- | --- | --- | --- |
| **6** | Engine | **Hash Join Operator** | ✅ DONE | Multi-Table Joins via Native Memory Maps. |
| **6** | Engine | **Group By Aggregation** | ✅ DONE | Multi-core Hash Aggregations. |
| **6** | SQL | **AST Pushdown Evaluator** | ✅ DONE | Pushes `AND`/`OR`/`<`/`>` down to C++ AVX. |
| **6** | SQL | **Late Materialization** | ✅ DONE | `ORDER BY` and `LIMIT` evaluated before row reconstruction. |

### **Phase 7: The HTAP Holy Grail (Current Focus)**

*OLTP Speed, Compaction, and Network APIS.*

| Phase | Module | Feature | Status | Impact |
| --- | --- | --- | --- | --- |
| **7** | Transact | **Primary Key Hash Index** | 📅 Pending | O(1) Point Lookups for instant `UPDATE` / `DELETE`. |
| **7** | Storage | **Background Compactor** | 📅 Pending | Clears "Ghost Rows" to restore pure AVX scan speeds. |
| **7** | Storage | **Dictionary Hydration** | 📅 Pending | Persists and loads String Dictionaries from disk. |
| **7** | Network | **Standalone Server Mode** | 📅 Active | Wire-protocol for external database clients. |

## 📊 Performance Metrics (Verified)

*Benchmarks run on 1 Million Rows (Transactional Mode).*

AwanDB utilizes a **Hybrid Scan** strategy. Performance varies depending on whether data blocks are "Clean" (immutable/compacted) or "Dirty" (contain active deletions/updates).

### ⚡ Transactional & Update Performance

| Workload | Throughput | Bandwidth | Notes |
| --- | --- | --- | --- |
| **Seq Scan (Clean)** | **16.1 Billion Rows/s** | **123 GB/s** | Pure AVX-512 Scan (Memory Saturated) |
| **Rand Read** | **569 Million Ops/s** | **4.3 GB/s** | Point lookups via Primary Index |
| **Trans. Write** | **2.43 Million Ops/s** | **~19 MB/s** | Full ACID Insert (WAL + Indexing) |
| **Rand Update** | **690,000 Ops/s** | **~5 MB/s** | Atomic Cycle: Index Lookup → Bitmap Mark → WAL → RAM Insert |
| **Seq Scan (Dirty)** | **92.8 Million Rows/s** | **707 MB/s** | Correctness Path (Scanning with Deletion Bitmaps) |

## 💻 Usage Examples

AwanDB allows you to define strict columnar schemas and query them using standard ANSI SQL. You can use it deeply embedded in your app, or run it as a standalone server.

### Example A: Embedded Mode (In-Memory / Zero Network Latency)

```scala
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler

// 1. Define Schema
val leaderboard = new AwanTable("leaderboard", capacity = 100000, dataDir = "./data")
leaderboard.addColumn("id")           
leaderboard.addColumn("player_name", isString = true)
leaderboard.addColumn("score")        

// 2. Register Table with SQL Engine
SQLHandler.register("leaderboard", leaderboard)

// 3. ACID Updates & Ingestion
SQLHandler.execute("INSERT INTO leaderboard VALUES (1, 'Alice', 250)")
SQLHandler.execute("INSERT INTO leaderboard VALUES (2, 'Bob', 900)")
SQLHandler.execute("UPDATE leaderboard SET score = 950 WHERE id = 1")

// 4. OLAP: Analytical Query 
val result = SQLHandler.execute("""
    SELECT player_name, score 
    FROM leaderboard 
    WHERE score > 200 
    ORDER BY score DESC 
    LIMIT 2
""")
println(result)

```

### Example B: Standalone Network Mode

AwanDB can be deployed as a dedicated database server, allowing external microservices, BI tools, and data pipelines to connect to it over the network.

**1. Start the Server:**

If you downloaded a release archive (e.g., `awandb-linux-x86_64.tar.gz` or `awandb-windows-x64.zip`):

1. Extract the archive.
2. Navigate to the extracted folder.
3. Run the standalone server using the provided wrapper scripts (which automatically link the C++ engine to the JVM):

```bash
# On Linux / macOS
./bin/awandb-server.sh --port {{Port Number}} --data-dir {{Data Directory}}

# On Windows
.\bin\awandb-server.bat --port {{Port Number}} --data-dir {{Data Directory}}
````

**2. Connect from a Client:**

```scala
import org.awandb.client.AwanClient

// Connect over TCP to the standalone database
val client = new AwanClient(host = "127.0.0.1", port = 5432)

// Send SQL statements over the wire
val response = client.query("SELECT player_name, score FROM leaderboard ORDER BY score DESC")
println(response.getRows)

client.disconnect()

```

## 📦 Releases & Distribution

Because AwanDB relies heavily on memory-mapped I/O, CPU-specific C++ intrinsics (AVX-512), and exact memory alignment via JNI, **we do not distribute AwanDB as a universal "Fat JAR".** Packaging native shared libraries (`.dll`, `.so`, `.dylib`) inside a single JVM artifact introduces unpredictable extraction overhead and OS-level security restrictions.

Instead, releases are distributed as **OS-Specific Pre-compiled Binaries**:

* `awandb-linux-x86_64.tar.gz` (Optimized for GCC/Clang on Linux)
* `awandb-windows-x64.zip` (Optimized for MSVC on Windows)
* `awandb-macos-arm64.tar.gz` (Apple Silicon compatible)

Each distribution contains the compiled JNI libraries perfectly matched to the bundled JVM artifacts, ensuring maximum hardware performance out-of-the-box.

## ⚙️ Build Instructions (From Source)

**1. Build Native Engine (C++)**

```bash
cd src/main/cpp
mkdir build && cd build
cmake -G "Visual Studio 17 2022" -A x64 ..  # Or `cmake ..` on Linux
cmake --build . --config Release

```

*Copy the resulting library to `lib/Release/`.*

**2. Compile Scala & Run Tests**

```bash
sbt test

```

## 📄 License

Copyright (c) 2026 Mohammad Iskandar Sham Bin Norazli Sham & Contributors.
This project is licensed under the Apache 2.0 License.