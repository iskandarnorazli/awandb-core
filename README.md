```markdown
# ☁️ AwanDB Core (OSS)

**The Standalone HTAP Database: Postgres Concurrency meets DuckDB Speed.**

AwanDB Core is an open-source, **Hybrid Transactional/Analytical Processing (HTAP)** engine. It is designed to handle high-velocity data ingestion (OLTP) and complex, blazing-fast reporting (OLAP) within a single unified runtime.

---

##  Quick Start: Running the Server

AwanDB is distributed as a highly optimized, pre-compiled native executable. Because it relies heavily on exact memory alignment and CPU-specific C++ intrinsics (AVX-512), no Java or compilation environment is required on your host machine.

**1. Download the Release**
Grab the latest OS-specific binary from the Releases tab:
* `awandb-linux-x86_64.tar.gz` (Optimized for GCC/Clang on Linux)
* `awandb-windows-x64.zip` (Optimized for MSVC on Windows)
* `awandb-macos-arm64.tar.gz` (Apple Silicon compatible)

**2. Start the Server**
Extract the archive and run the executable. You can customize the port and data directory:

```bash
# On Linux / macOS
./bin/awandb-server --port 3000 --data-dir ./data/default

# On Windows
.\bin\awandb-server.exe --port 3000 --data-dir ./data/default

```

### Authentication (Basic Auth)

AwanDB strictly enforces authentication. By default, the database is secured using Basic Authentication. You must pass these credentials in the header of every client connection.

* **Default Username:** `admin`
* **Default Password:** `admin`

---

## Connecting & Using AwanDB (Python / Flight SQL)

AwanDB communicates entirely over **Apache Arrow Flight SQL**, an ultra-fast RPC protocol. While the example below uses Python, any language with a Flight SQL client (Go, Rust, Java, Node.js) will work exactly the same way.

### Payloads & Responses (Column-Oriented)
Because AwanDB is a columnar database communicating via Arrow, data is strictly transmitted in vectorized batches rather than row-by-row JSON arrays. 
* **SQL Queries:** Standard SQL executions return a single string-based Arrow Vector named `query_result`. This column contains the formatted query output or execution status messages.
* **Raw Binary Ingestion:** For extreme throughput bypassing the SQL parser entirely, AwanDB can accept raw Apache Arrow `IntVector` streams directly into its columnar memory model via the `DoPut` endpoint.

### Python Client Example

First, install the required Apache Arrow and ADBC libraries:
```bash
pip install pyarrow adbc-driver-flightsql

```

Next, run the following script to see both Standard SQL and Raw Binary ingestion in action:

```python
import base64
import pyarrow as pa
import pyarrow.flight as flight
from adbc_driver_flightsql import dbapi

# AwanDB enforces Basic Auth. Encode credentials for the headers.
auth_str = base64.b64encode(b"admin:admin").decode("utf-8")
basic_auth_header = f"Basic {auth_str}"

# =========================================================
# PHASE 1: STANDARD FLIGHT SQL (ADBC Driver)
# =========================================================
print("-> Connecting via ADBC Flight SQL...")

# Connect using standard Python DB-API
conn = dbapi.connect(
    "grpc://localhost:3000",
    db_kwargs={
        "adbc.flight.sql.rpc.call_header.Authorization": basic_auth_header,
    }
)
cursor = conn.cursor()

# Execute Standard SQL (DDL & DML)
cursor.execute("CREATE TABLE users (id INT, value INT)")
print(cursor.fetchall())

cursor.execute("INSERT INTO users VALUES (1, 100)")
cursor.execute("INSERT INTO users VALUES (2, 200)")
cursor.execute("UPDATE users SET value = 999 WHERE id = 1")

# Execute Complex Aggregations
cursor.execute("SELECT COUNT(*) AS total, SUM(value) AS total_val FROM users WHERE value > 150")
print(cursor.fetchall())

conn.close()


# =========================================================
# PHASE 2: RAW ARROW STREAM (Zero-Copy Binary Ingestion)
# =========================================================
print("\n-> Connecting via Raw Flight RPC for High-Speed Ingestion...")

client = flight.FlightClient("grpc://localhost:3000")
options = flight.FlightCallOptions(headers=[(b"authorization", basic_auth_header.encode("utf-8"))])

# Target an existing table directly by path (bypassing the SQL parser)
descriptor = flight.FlightDescriptor.for_path("leaderboard")
schema = pa.schema([('val', pa.int32())])

# Open a DoPut stream
writer, _ = client.do_put(descriptor, schema, options=options)

# Blast native Arrow RecordBatches directly into memory
batch = pa.RecordBatch.from_arrays([pa.array([10, 20, 30, 40, 50], type=pa.int32())], schema=schema)
writer.write_batch(batch)
writer.close()

print("✅ Data ingested successfully!")

```

```

```

## Supported SQL Dialect

AwanDB features a built-in ANSI SQL parser mapped directly to a Volcano execution model, pushing operations down into native C++ bitmasks.

### DDL (Data Definition)

* **CREATE TABLE:** `CREATE TABLE table_name (id INT, status STRING)`
* **DROP TABLE:** `DROP TABLE table_name`
* **ALTER TABLE:** `ALTER TABLE table_name ADD new_column INT`

### DML (Data Manipulation)

* **INSERT:** * Full: `INSERT INTO users VALUES (1, 'Alice')`
* Partial: `INSERT INTO users (name) VALUES ('Bob')`
* *Supports `RETURNING` clauses.*


* **UPDATE:** `UPDATE users SET score = 1000 WHERE id = 1`
* **DELETE:** `DELETE FROM users WHERE score < 50`

### DQL (Data Querying)

AwanDB supports complex reporting queries including compound predicates, late materialization, and scalar aggregations.

* **WHERE Clauses:** Supports `=`, `>`, `>=`, `<`, `<=`, `AND`, `OR`, `IN (...)`, `LIKE`, `IS NULL`, and `IS NOT NULL`.
* **ORDER BY & LIMIT:** `SELECT * FROM users ORDER BY score DESC LIMIT 5`
* **GROUP BY:** `SELECT status, SUM(score) FROM users GROUP BY status`
* **JOIN:** `SELECT * FROM users JOIN scores ON users.id = scores.user_id`
* **Aggregations:** Supports `COUNT`, `SUM`, `MAX`, `MIN`, and `AVG`.

---

## Architecture & Project Vision

> **Why run two databases?**
> Typically, developers write to a transactional DB (like Postgres) and sync data to an analytical DB (like DuckDB/ClickHouse) for reporting.
> **AwanDB unifies this.** It accepts ACID transactions at millions of ops/sec and executes analytical queries on that same data microseconds later, using raw C++ AVX-512 intrinsics.

### Key Features

* **True HTAP Architecture:**
* **OLTP:** Async, actor-model ingestion handles massive concurrency.
* **OLAP:** Vectorized C++ kernels scan data at memory bandwidth limits (>100 GB/s).


* **Hardware-Accelerated Predicate Pushdown:** AST evaluations (like `WHERE score > 100`) are pushed down into native C++ bitmasks using `_BitScanForward` and AVX-512 intrinsics.
* **Hybrid Scan Strategy:**
* **Fast Path:** Blocks with no deletions are scanned via raw AVX-512 instructions (Clean Scan).
* **Safe Path:** Blocks with active deletions use a bitmap filter to ensure Snapshot Isolation (Dirty Scan).



---

## Performance Metrics (Verified)

*Benchmarks run on 1 Million Rows (Transactional Mode).*

AwanDB utilizes a **Hybrid Scan** strategy. Performance varies depending on whether data blocks are "Clean" (immutable/compacted) or "Dirty" (contain active deletions/updates).

| Workload | Throughput | Bandwidth | Notes |
| --- | --- | --- | --- |
| **Seq Scan (Clean)** | **5.89 Billion Rows/s** | **45 GB/s** | Pure AVX-512 Scan (Memory Saturated) |
| **Rand Read** | **136 Million Ops/s** | **1 GB/s** | Point lookups via Primary Index |
| **Trans. Write** | **2.26 Million Ops/s** | **~19 MB/s** | Full ACID Insert (WAL + Indexing) |
| **Rand Update** | **870,000 Ops/s** | **~6 MB/s** | Atomic Cycle: Index Lookup → Bitmap Mark → WAL → RAM Insert |
| **Seq Scan (Dirty)** | **176 Million Rows/s** | **1.4 GB/s** | Correctness Path (Scanning with Deletion Bitmaps) |

---

## License

Copyright (c) 2026 Mohammad Iskandar Sham Bin Norazli Sham & Contributors.
This project is licensed under the Apache 2.0 License.