# AwanDB User Guide & HOWTO

This guide provides a comprehensive overview for installing, running, and interacting with **AwanDB**.

AwanDB is a hybrid HTAP engine: a high-performance **C++ Native Backend** (for AVX-512 vector compute) wrapped in a **Scala Frontend** (for SQL parsing, query planning, and networking).

---

### **1. Prerequisites**

Before running AwanDB, ensure your environment is set up correctly:

* **JDK 21 (or 24+)**: The engine is strictly compiled for modern Java foreign memory access and advanced JNI boundaries.
* *Verify:* `java -version` should show `21...` or higher.


* **OS-Specific Binaries**: AwanDB relies heavily on hardware-specific memory alignment and CPU intrinsics. You must download the correct release archive for your OS (Linux, Windows, or macOS).

---

### **2. Installation & Running**

AwanDB is no longer distributed as a slow "Fat JAR". We distribute pre-compiled, OS-optimized archives.

#### **Option A: Production / Standalone Mode (Recommended)**

If you downloaded a release archive (e.g., `awandb-linux-x86_64.tar.gz` or `awandb-windows-x64.zip`):

1. Extract the archive.
2. Navigate to the extracted folder.
3. Run the standalone server using the provided wrapper scripts (which automatically link the C++ engine to the JVM):

```bash
# On Linux / macOS
./bin/awandb-server.sh

# On Windows
.\bin\awandb-server.bat

```

#### **Option B: Developer Mode (From Source)**

If you are developing or testing locally from the repository:

```bash
# 1. Compile the C++ Engine first!
cd src/main/cpp && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release && cmake --build . --config Release

# 2. Return to root and run the Interactive SQL Console via SBT
cd ../../../
sbt "runMain org.awandb.core.MainApp"

# Or start the network server:
sbt "runMain org.awandb.server.AwanServer"

```

---

### **3. SQL Capabilities (The "What Works" Guide)**

As of Phase 6, the `SQLHandler` is a fully powered **Volcano Execution Pipeline**. It parses standard ANSI SQL and compiles it down into native C++ SIMD operations.

#### **✅ Supported DML (Data Manipulation)**

| Command | Syntax Example | Notes |
| --- | --- | --- |
| **INSERT** | `INSERT INTO users VALUES (1, 'Alice', 25)` | Types must match the registered table schema. |
| **DELETE** | `DELETE FROM users WHERE id = 1` | Translates to an O(1) bitmap flip in C++. |
| **UPDATE** | `UPDATE users SET age = 26 WHERE id = 1` | Executed safely as an atomic Delete + Insert. |

#### **✅ Supported DQL (Data Querying)**

AwanDB supports complex analytical queries, pushing predicates down to the hardware level before performing Late Materialization.

| Feature | Syntax Example | Under the Hood |
| --- | --- | --- |
| **SELECT & Project** | `SELECT player_name, score FROM leaderboard` | Extracts specific columns without loading full rows into memory. |
| **AVX Pushdown** | `WHERE score > 1000` | Filters (`<`, `>`, `<=`, `>=`, `=`) are evaluated inside C++ using `_mm256` vector instructions. |
| **Compound Logic** | `WHERE age < 30 AND score > 500` | Supports complex AST parsing, respecting standard `AND` / `OR` operator precedence. |
| **Hash Joins** | `SELECT * FROM users JOIN orders ON users.id = orders.user_id` | Performs blazing-fast in-memory Hash Joins across multiple tables. |
| **Aggregations** | `SELECT SUM(score) FROM users GROUP BY team_id` | Executes multi-core Hash Aggregations natively. |
| **Sort & Limit** | `ORDER BY score DESC LIMIT 10` | Applies sorting and paging *before* tuple reconstruction (Late Materialization) to save memory. |

---

### **4. Advanced Usage: The Scala Operator API**

If you want to bypass the SQL parser entirely (e.g., for programmatic query generation or extreme performance tuning), you can chain the underlying execution operators directly in Scala.

The SQL parser actually builds this exact tree under the hood!

**Example: High-Performance Scan & Aggregation Pipeline**

```scala
import org.awandb.core.query._
import org.awandb.core.storage.BlockManager

// 1. Define the physical Table Scan (pushes down to native blocks)
val scanOp = new TableScanOperator(
  blockManager, 
  blocks = blockManager.getLoadedBlocks.toArray, 
  keyColIdx = 0, 
  valColIdx = 1
)

// 2. Wrap in an Aggregation Node (GROUP BY key SUM(val))
val aggOp = new HashAggOperator(scanOp)

// 3. Execute the Volcano Pipeline
aggOp.open()
var batch = aggOp.next()

while (batch != null) {
  // Process high-speed C++ VectorBatch outputs
  println(s"Processed ${batch.count} aggregated groups.")
  batch = aggOp.next()
}

aggOp.close()

```

---

### **5. Arrow Flight Server (Network Mode)**

AwanDB includes an `AwanFlightServer` that starts a gRPC endpoint on **port 3000**.

* **Capabilities:** Currently set up as a **Network Throughput Demo**.
* **Usage:** `AwanFlightProducer.scala` generates synthetic stream data (`row_0`, `row_1`...) to test maximum wire-speed to connected clients.
* *Note:* Hooking this up to the live `SQLHandler` is scheduled for the upcoming Network API phase.

[Introduction to Apache Arrow Flight](https://www.google.com/search?q=https://www.youtube.com/watch%3Fv%3DF0O5R5KScs4)
*This video explains the Arrow Flight protocol used in `AwanFlightServer`, clarifying why it is significantly faster than standard JDBC/ODBC connections.*

---

### **6. Troubleshooting**

* **`UnsatisfiedLinkError` or `java.lang.NoClassDefFoundError**`: The JVM cannot find the C++ library.
* *Fix:* Ensure you are launching the application using `./bin/awandb-server.sh`. This script dynamically injects `-Djava.library.path=...` to link the native libraries. If running via SBT, pass `-Djava.library.path=./build/Release`.


* **JVM Crash (`EXCEPTION_ACCESS_VIOLATION`)**:
* *Fix A:* You are likely running **Java 11/17**. You **must** switch to **Java 21+**.
* *Fix B:* If developing custom C++ kernels, ensure your Scala `VectorBatch` capacity matches the C++ `allocMainStore` exactly, otherwise you will trigger a native buffer overflow.