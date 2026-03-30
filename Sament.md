```markdown
# AwanDB AI Agent & Developer Configuration (`agents.md`)

[cite_start]This document outlines the strict architectural guardrails, lifecycle management rules, and security protocols for AwanDB[cite: 39, 49]. [cite_start]Guardrails must be applied situationally based on the subsystem being modified[cite: 49].

## Scenario: General Architecture, Security & Documentation
* [cite_start]**Local Execution Context**: Always use the Ephemeral Engine for local operations[cite: 39].
* [cite_start]**Network Security**: Never send plaintext passwords from the client[cite: 40]. [cite_start]Payloads must be secured to prevent MITM attacks[cite: 40].
* [cite_start]**Mermaid Diagram Generation**: When creating flowcharts for authentication, strictly map out the legacy authentication pathways[cite: 41]. [cite_start]Do not include the current implementation with versioning[cite: 42]. Ensure standard ASCII arrows (`-->`) are used to prevent lexical errors in the parser.

## Scenario: Native Memory Allocation & JNI Boundaries (C++ / Scala)
* [cite_start]**Memory Alignment**: Native block memory must be 64-byte aligned for Cache Line and AVX-512/SIMD efficiency[cite: 44, 279, 283]. 
* [cite_start]**Allocation Functions**: Strictly use `alloc_aligned()` and `free_aligned()` defined in `common.h`[cite: 45, 281]. [cite_start]Never use standard C++ `new`/`delete` or `malloc`/`free` for block data[cite: 45, 280].
* [cite_start]**JNI Critical Sections**: When accessing JVM arrays in C++ via `GetPrimitiveArrayCritical`, you must release them immediately with mode `0`[cite: 46, 297, 298]. [cite_start]Do not perform blocking operations, context switches, or heavy allocations inside these blocks to prevent stalling the JVM garbage collector[cite: 47, 299].
* [cite_start]**Pointer Wrapping**: Ensure C++ pointers returned to Scala are safely wrapped in a memory lifecycle manager[cite: 48, 301]. [cite_start]Unmanaged native pointers will create "ghost objects" resulting in memory leaks and OOM crashes[cite: 302, 303]. [cite_start]Consider utilizing Java's Cleaner API or Phantom References for guaranteed automated reclamation[cite: 411].

## Scenario: Query Execution & Volcano/Morsel Pipelines
* [cite_start]**Thread-Local Buffers**: Queries are executed in parallel chunks[cite: 49]. [cite_start]Ensure thread-local buffers are isolated[cite: 50]. [cite_start]Use `NativeBridge.freeMainStore()` to release temporary query buffers and avoid cross-thread pointer contamination[cite: 50, 330]. [cite_start]Ensure this is called even if a query thread aborts to prevent permanent leaks[cite: 332, 333].
* [cite_start]**Morsel Execution Boundaries**: Ensure that `MorselExec` chunks strictly respect block bounds[cite: 56, 356]. [cite_start]Out-of-bounds pointer arithmetic during AVX/NEON vectorized execution will cause immediate hardware-level Segmentation Faults (SIGSEGV)[cite: 56, 357, 359].

## Scenario: High-Velocity Data Ingestion (Delta Store)
* [cite_start]**Explicit Buffer Clearance**: Active ingestion happens in RAM within the Delta Store[cite: 54, 320]. [cite_start]You must clear the Delta Memory buffers explicitly after flushing data to persistent disk[cite: 54, 323].
* [cite_start]**Required Invocations**: Failure to invoke clearance protocols results in RAM overflow and silent data duplication[cite: 325, 326]. Always execute:
  ```scala
  col.clearDelta()
  col.deltaIntBuffer.clear()
  col.deltaStringBuffer.clear()
  ```

## Scenario: Epoch-Based Memory Management (EBMM) & Concurrency
* [cite_start]**Safe Reclamation**: Memory reclamation must only occur when it is mathematically proven to be safe from concurrent access[cite: 51, 368].
* [cite_start]**Epoch Registration**: Tie asynchronous operations (like vector searches or background compactions) strictly to the `EpochManager`[cite: 52, 369].
* [cite_start]**Preventing Use-After-Free**: Use `table.epochManager.tryReclaim()`[cite: 53, 369]. [cite_start]Never immediately free physical blocks that might be scanned by an active thread, as this will trigger catastrophic Use-After-Free race conditions[cite: 53, 370].

## Scenario: Teardowns & Complex Data Structure Destruction
* [cite_start]**AwanTable Cascades**: When dropping or closing an `AwanTable`, call `table.close()` to cascade memory reclamation down to `NativeMemoryReleaser` and `BlockManager`[cite: 57, 419]. [cite_start]Ensure this cascade is protected against runtime exceptions[cite: 420].
* [cite_start]**Arrow Flight Vectors**: For Zero-Copy Arrow Flight vectors, ensure off-heap Netty arenas (`VarCharVector` and `Float8Vector` pools) are explicitly closed via methods like `root.close()` and `allocator.close()`[cite: 58, 418].
* [cite_start]**Graph Engine Matrices**: Native Graph matrices (Compressed Sparse Row formats) must have explicit destructors called (e.g., `graph.close()`)[cite: 59, 413, 414]. [cite_start]Failure to destroy native CSR matrices causes instant JVM Segmentation Faults when memory is freed improperly[cite: 60, 415].

## Scenario: Writing Isolated Test Suites
* [cite_start]**Test Class Setup**: To completely isolate tests and prevent C++ state leakage across runs, test classes must implement `BeforeAndAfterEach`[cite: 42].
* [cite_start]**Verbatim Hook Requirements**: Copy the following verbatim for test setups to clear directories and SQL registries[cite: 43, 424]:
  ```scala
  override def beforeEach(): Unit = {
    deleteRecursively(new File("data/flight_test"))
    deleteRecursively(new File("data/flight_zerocopy"))
    SQLHandler.tables.clear()
  }

  override def afterEach(): Unit = {
    deleteRecursively(new File("data/flight_test"))
    deleteRecursively(new File("data/flight_zerocopy"))
  }
  ```
```