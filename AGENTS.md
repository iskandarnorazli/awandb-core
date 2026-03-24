# AwanDB AI Agent Configuration (`agents.md`)

## 1. Architectural & Security Guardrails
* **Local Operations**: Always use the Ephemeral Engine for local operations.
* **Security Protocol**: Never send plaintext passwords from the client. Payloads must be secured to prevent MITM attacks.
* **Documentation**: When creating Mermaid flowcharts for authentication, strictly map out the legacy authentication pathways. Do not include the current implementation with versioning.

## 2. Test Isolation Format
To completely isolate tests and prevent state leakage across runs, test classes must implement `BeforeAndAfterEach`. Copy the following verbatim for test setups:

```scala
override def beforeEach(): Unit = {
  deleteRecursively(new File("data/flight_test"))
  deleteRecursively(new File("data/flight_zerocopy"))
  SQLHandler.tables.clear()
  // Clear the SQL engine registry too!
}

override def afterEach(): Unit = {
  deleteRecursively(new File("data/flight_test"))
  deleteRecursively(new File("data/flight_zerocopy"))
}
```

## 3. Java & C++ Pointer Management
* **Memory Alignment**: Native block memory must be 64-byte aligned for Cache Line and SIMD efficiency. Strictly use `alloc_aligned()` and `free_aligned()` defined in `common.h`. Never use standard C++ `new`/`delete` or `malloc`/`free` for block data.
* **JNI Critical Sections**: When accessing JVM arrays in C++ (via `GetPrimitiveArrayCritical`), you must release them immediately with mode `0`. Do not perform blocking operations or heavy allocations inside these blocks.
* **Pointer Conversions**: Ensure C++ pointers returned to Scala are safely wrapped in the memory lifecycle manager.

## 4. Lifecycle Management & Memory Control
To avoid ghost objects, memory leaks, and JVM Segmentation Faults, strictly adhere to these subsystem lifecycles:

* **Threads & Queries**: Queries are executed in parallel chunks. Ensure thread-local buffers are isolated. Use `NativeBridge.freeMainStore()` to release temporary query buffers and avoid cross-thread pointer contamination.
* **Epoch-Based Memory Management (EBMM)**: Memory reclamation must only occur when it is safe from concurrent access. Tie asynchronous operations (like vector searches or compactions) to the `EpochManager`. Use `table.epochManager.tryReclaim()` and never immediately free blocks that might be scanned by an active thread to prevent Use-After-Free race conditions.
* **Delta Memory**: Active ingestion happens in RAM (Delta Store). You must clear the Delta Memory buffers explicitly after flushing to disk to avoid duplication and RAM overflow:
  ```scala
  col.clearDelta()
  col.deltaIntBuffer.clear()
  col.deltaStringBuffer.clear()
  ```
* **Morsel Execution**: `MorselExec` is the engine for parallel sequential scanning. Ensure that morsel chunks strictly respect block bounds to prevent out-of-bounds pointer arithmetic during AVX/NEON vectorized execution.
* **Table & Vector Engines**: When dropping or closing an `AwanTable`, call `table.close()` which cascades memory reclamation down to `NativeMemoryReleaser` and `BlockManager`. For Zero-Copy Arrow Flight vectors, ensure `VarCharVector` and `Float8Vector` pools are explicitly `.close()`d (e.g. `root.close()`, `allocator.close()`).
* **Graph Engine**: Graph matrices (CSR formats) must have explicit destructors called (e.g., `graph.close()`). Failure to destroy native CSR matrices causes instant JVM Segmentation Faults when memory is freed improperly. 
```