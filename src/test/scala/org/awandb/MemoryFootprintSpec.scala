/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.awandb.core.engine.{AwanTable, QueryExecutor}
import org.awandb.core.query.{ScanOperator, Operator}
import org.awandb.core.jni.NativeBridge
import org.awandb.core.engine.memory.NativeMemoryTracker
import org.awandb.core.sql.SQLHandler
import java.io.File
import java.lang.management.ManagementFactory

class MemoryFootprintSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TEST_DIR = "data/memory_profile_db"
  private var table: AwanTable = _

  // --- MEMORY MEASUREMENT HELPERS ---
  
  private def getJvmUsedMB(): Long = {
    System.gc() // Suggest a GC before taking a JVM snapshot for accuracy
    Thread.sleep(50)
    val runtime = Runtime.getRuntime
    (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
  }

  private def getNativeUsedMB(): Long = {
    NativeMemoryTracker.getActiveBytes / (1024 * 1024)
  }

  private def getWalSizeMB(): Long = {
    val walFile = new File(s"$TEST_DIR/segment_0.wal")
    if (walFile.exists()) walFile.length() / (1024 * 1024) else 0L
  }

  private def cleanDir(path: String): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      val files = dir.listFiles()
      if (files != null) files.foreach(_.delete())
      dir.delete()
    }
  }

  // Asks the OS Kernel (Windows/Linux) for the actual Process Memory footprint
  private def getOsCommittedMB(): Long = {
    val bean = ManagementFactory.getOperatingSystemMXBean
    bean match {
      case sunBean: com.sun.management.OperatingSystemMXBean =>
        sunBean.getCommittedVirtualMemorySize / (1024 * 1024)
      case _ => -1L
    }
  }

  override def beforeEach(): Unit = {
    cleanDir(TEST_DIR)
    NativeBridge.init()
    // [CRITICAL FIX] Increase to 250MB to prevent Deadlocks when flushing massive blocks!
    NativeBridge.initTestBufferPool(250 * 1024 * 1024) 
  }

  override def afterEach(): Unit = {
    if (table != null) {
      try { 
        table.drop()
        Thread.sleep(100) // Give detached drop threads time to clean up
      } catch { case _: Exception => }
    }
    NativeBridge.destroyTestBufferPool()
    cleanDir(TEST_DIR)
    
    // Catch any orphaned pointers at the end of every test
    NativeMemoryTracker.assertNoLeaks()
  }

  "AwanDB Memory Architecture" should "properly transition data between WAL, Delta, Main Memory, and Query Arenas" in {
    val baselineJvm = getJvmUsedMB()
    val baselineNative = getNativeUsedMB()
    
    table = new AwanTable("footprint_test", 100000, TEST_DIR, daemonIntervalMs = 5000L)
    table.addColumn("col_int")
    table.addColumn("col_str", isString = true)

    // PHASE 2: Ingestion
    val rowCount = 500000
    for (i <- 1 to rowCount) table.insertRow(Array(i, s"user_data_string_$i"))

    val deltaJvm = getJvmUsedMB()
    val deltaNative = getNativeUsedMB()
    val walSize = getWalSizeMB()
    
    deltaJvm should be > baselineJvm
    walSize should be > 0L
    deltaNative shouldEqual baselineNative 

    // PHASE 3: Flush to C++ Main Memory
    table.flush() 

    val mainJvm = getJvmUsedMB()
    val mainNative = getNativeUsedMB()
    val postWalSize = getWalSizeMB()

    mainNative should be > deltaNative
    postWalSize shouldEqual 0L
    mainJvm should be < deltaJvm

    // PHASE 4: Query Execution (Volcano Iterator)
    val queryId = "test-query-arena-1"
    val dummyData = (1 to 5000).toArray
    val scanOp = new ScanOperator(dummyData, 1024)
    val stream = QueryExecutor.executeStream(queryId, scanOp)
    
    if (stream.hasNext) stream.next()
    while (stream.hasNext) stream.next()

    val postQueryNative = getNativeUsedMB()
    postQueryNative shouldEqual mainNative
  }

  // =========================================================================
  // REGRESSION TESTS: CATCHING THE PYTHON / FLIGHT SQL BUGS
  // =========================================================================

  it should "execute DROP TABLE instantly (0ms latency) without blocking on background compactions" in {
    table = new AwanTable("drop_latency_test", 1000, TEST_DIR)
    table.addColumn("col_int")
    table.insertRow(Array(1))
    table.flush()

    // 1. Simulate the background Daemon currently executing a massive C++ block compaction
    table.rwLock.readLock().lock()
    
    val dropStartTime = System.nanoTime()
    
    // 2. Execute Drop (Should detach to a new thread instantly)
    table.drop() 
    
    val dropEndTime = System.nanoTime()
    val dropDurationMs = (dropEndTime - dropStartTime) / 1000000.0

    // 3. Release the lock so the detached background thread can actually wipe the memory
    table.rwLock.readLock().unlock()
    Thread.sleep(200) // Give cleanup thread time to finish before afterEach() leak checks

    println(f"[Test] Detached Drop executed in $dropDurationMs%.2f ms")
    
    // If the Drop is synchronous (the bug), it will wait for the lock and timeout/fail here.
    dropDurationMs should be < 50.0 
  }

  it should "reclaim all orphaned pointers in the EpochManager during DROP TABLE to prevent native leaks" in {
    table = new AwanTable("epoch_leak_test", 10000, TEST_DIR)
    table.addColumn("col_int")
    
    // 1. Create a block on disk
    for (i <- 1 to 5000) table.insertRow(Array(i))
    table.flush()

    // 2. Delete rows. This allocates native bitmasks and marks the block as dirty
    for (i <- 1 to 2500) table.delete(i)
    
    // 3. Force compaction. This swaps the blocks and sends the old Block, Bitmask, 
    //    and Cuckoo Filters to the EpochManager's retirement queue.
    table.compactor.compact(0.1)
    
    // Ensure the queue actually has items waiting to be freed
    val pending = table.epochManager.getPendingReclaims
    println(s"[Test] Pointers waiting in EpochManager queue before drop: $pending")
    pending should be > 0

    // 4. Drop the table. If EpochManager isn't forcibly flushed, these pointers leak!
    table.drop()
    Thread.sleep(200) // Wait for detached drop thread
    
    // The afterEach() NativeMemoryTracker.assertNoLeaks() will throw here if it failed.
  }

  "SQLHandler" should "safely infer multi-column schemas for Arrow Flight without truncation" in {
    table = new AwanTable("schema_test", 1000, TEST_DIR)
    table.addColumn("product_id")
    table.addColumn("sales_amount")
    SQLHandler.register("olap_sales_1gb", table) // Register with the exact table name from the Python script

    // The exact query from the failing Python test
    val sql = "SELECT product_id, COUNT(*) as sales FROM olap_sales_1gb GROUP BY product_id ORDER BY sales DESC LIMIT 5"
    
    // 1. Ask the engine to infer the schema
    val schema = SQLHandler.inferSchema(sql)

    println(s"[Test] Inferred Schema: ${schema.map(t => s"${t._1} (${t._2})").mkString(", ")}")
    
    // 2. If the JSqlParser version incompatibility bug exists, this will equal 1 ("query_result")
    schema.length shouldBe 2
    
    // 3. Validate correct extraction
    schema(0)._1 shouldBe "product_id"
    schema(0)._2 shouldBe "INT"
    schema(1)._1 shouldBe "sales"
    schema(1)._2 shouldBe "STRING"
  }

  it should "force the OS to reclaim phantom native memory after a table DROP (_heapmin / malloc_trim)" in {
    val startNativeLedger = getNativeUsedMB()
    val startOsMem = getOsCommittedMB()
    
    println(f"\n[OS Memory Test] Baseline -> Ledger: $startNativeLedger MB | OS Committed: $startOsMem MB")

    table = new AwanTable("os_trim_test", 100_000, TEST_DIR)
    table.addColumn("col_int")
    table.addColumn("col_str", isString = true)

    // 1. Allocate data to bloat the C++ heap without choking the JNI bridge!
    // 25,000 rows * ~1KB string = ~25MB of pure C++ String Pool allocation
    val rowCount = 25_000
    val massivePadding = "X" * 1000 
    
    for (i <- 1 to rowCount) {
      table.insertRow(Array(i, s"${massivePadding}_$i"))
    }
    table.flush()
    
    val peakNativeLedger = getNativeUsedMB()
    val peakOsMem = getOsCommittedMB()
    println(f"[OS Memory Test] Peak     -> Ledger: $peakNativeLedger MB | OS Committed: $peakOsMem MB")
    
    // Ensure the data actually bloated the memory
    peakNativeLedger should be > startNativeLedger
    if (peakOsMem != -1L) peakOsMem should be > startOsMem
    
    // 2. Drop the table (Triggers NativeBridge.trimMemory() via detached thread)
    table.drop()
    
    // 3. Wait for the detached background thread to execute the physical free() and trim()
    Thread.sleep(1500) 
    
    // 4. Verify the Internal Native Ledger is completely empty
    val finalNativeLedger = getNativeUsedMB()
    finalNativeLedger shouldBe startNativeLedger
    
    // 5. Verify the OS actually reclaimed the Phantom Leak
    val finalOsMem = getOsCommittedMB()
    println(f"[OS Memory Test] Final    -> Ledger: $finalNativeLedger MB | OS Committed: $finalOsMem MB\n")
    
    if (finalOsMem != -1L) {
      finalOsMem should be < peakOsMem
    }

    // 6. Explicitly invoke trimMemory to prove the JNI bridge binds correctly
    noException should be thrownBy {
      org.awandb.core.jni.NativeBridge.trimMemory()
    }
  }

  // =========================================================================
  // CONTINUOUS HTAP CHURN & CONCURRENCY TESTS
  // =========================================================================

  it should "maintain stable native and OS memory during continuous HTAP churn (Insert/Delete/Compact/Query)" in {
    // 1. Initialize table with a fast daemon interval for testing
    table = new AwanTable("htap_churn_test", 1000, TEST_DIR, daemonIntervalMs = 100L)
    table.addColumn("col_int")
    table.addColumn("col_str", isString = true)

    println("\n[HTAP Churn] Warming up engine working set...")

    // 2. Warmup Phase: Let JVM and C++ settle into a baseline working set
    for (_ <- 1 to 5) {
      for (i <- 1 to 2000) table.insertRow(Array(i, s"warmup_padding_string_$i"))
      table.flush()
      for (i <- 1 to 500) table.delete(i)
      table.compactor.compact(0.1)
      table.query(-1)
      Thread.sleep(100) // Yield to Background Daemon
    }

    val midNative = getNativeUsedMB()
    val midOs = getOsCommittedMB()
    println(f"[HTAP Churn] Baseline established -> Ledger: $midNative MB | OS Committed: $midOs MB")

    println("[HTAP Churn] Simulating heavy continuous operations...")

    // 3. Stress Phase: Do 20 cycles of heavy churn. 
    // If EpochManager or Compactor leaks, memory will climb linearly and balloon.
    for (cycle <- 1 to 20) {
      // A. Heavy Inserts
      for (i <- 1 to 2000) table.insertRow(Array(i, s"stress_padding_string_xyz_$i"))
      table.flush()
      
      // B. Heavy Deletes (creates fragmentation and native bitmasks)
      for (i <- 1 to 800) table.delete(i)
      
      // C. Compact (Retires old blocks and bitmasks to EpochManager)
      table.compactor.compact(0.1)
      
      // D. Query (Allocates Query Arena, tears it down, and invokes OS malloc_trim)
      table.query(-1)
      
      // Yield slightly to ensure the detached daemon processes the Epoch queue
      Thread.sleep(50)
    }

    // Force one last sync
    table.epochManager.advanceGlobalEpoch()
    table.epochManager.tryReclaim()
    org.awandb.core.jni.NativeBridge.trimMemory()
    Thread.sleep(500)

    val finalNative = getNativeUsedMB()
    val finalOs = getOsCommittedMB()
    println(f"[HTAP Churn] After Stress     -> Ledger: $finalNative MB | OS Committed: $finalOs MB\n")

    // 4. Assertions: Memory must Plateau!
    // Native ledger should be extremely tight. (Allow a tiny 5MB variance for active blocks).
    finalNative should be <= (midNative + 5L) 
    
    // OS memory should not have climbed linearly. 
    // If it leaked, it would be 100MB+ larger. We allow a 30MB variance for JVM/glibc fragmentation.
    if (finalOs != -1L && midOs != -1L) {
       finalOs should be <= (midOs + 30L) 
    }
  }

  it should "not leak Query Context arenas during highly concurrent OLAP read workloads" in {
    table = new AwanTable("query_arena_test", 1000, TEST_DIR)
    table.addColumn("val")
    for (i <- 1 to 5000) table.insertRow(Array(i))
    table.flush()

    // Measure exact native bytes before the query storm
    val startNativeBytes = NativeMemoryTracker.getActiveBytes
    println(f"\n[Query Storm] Pre-Query Native Ledger: $startNativeBytes bytes")

    import scala.concurrent.{Future, Await}
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    // Fire 10,000 parallel queries
    // This forces C++ to dynamically allocate and destroy 10,000 std::vector tracking arenas concurrently.
    val queries = (1 to 10000).map { _ =>
      Future {
        table.query(2500)
      }
    }
    
    // Wait for all 10,000 queries to complete
    Await.result(Future.sequence(queries), 30.seconds)

    // Measure exact native bytes after the storm
    val finalNativeBytes = NativeMemoryTracker.getActiveBytes
    println(f"[Query Storm] Post-Query Native Ledger: $finalNativeBytes bytes\n")

    // The native memory should return EXACTLY to the byte it started at!
    finalNativeBytes shouldEqual startNativeBytes
  }

  it should "prevent JVM ArrayBuffer capacity hoarding and release heap memory back to the OS" in {
    // 1. Force a clean slate to get an accurate JVM baseline
    System.gc()
    Thread.sleep(200)

    val startJvm = getJvmUsedMB()
    val startOs = getOsCommittedMB()
    
    println(f"\n[JVM Hoarding Test] Baseline -> JVM Used: $startJvm MB | OS Committed: $startOs MB")

    table = new AwanTable("jvm_hoard_test", 100_000, TEST_DIR)
    table.addColumn("col_int")
    table.addColumn("col_str", isString = true)

    // 2. Bloat the JVM ArrayBuffers intentionally!
    // 100,000 rows with 1KB strings = massive JVM String and Array allocation
    val rowCount = 100_000
    val massivePadding = "J" * 1000 
    
    for (i <- 1 to rowCount) {
      table.insertRow(Array(i, s"${massivePadding}_$i"))
    }
    
    // NOTE: We deliberately do NOT call table.flush() here. 
    // We want the Scala ArrayBuffers inside NativeColumn to be at absolute maximum physical capacity.
    
    val peakJvm = getJvmUsedMB()
    val peakOs = getOsCommittedMB()
    println(f"[JVM Hoarding Test] Peak     -> JVM Used: $peakJvm MB | OS Committed: $peakOs MB")
    
    peakJvm should be > startJvm
    
    // 3. Drop the table. 
    // Buggy Engine: calls buffer.clear() -> JVM hoards the array size forever.
    // Fixed Engine: calls new ArrayBuffer() + System.gc() -> JVM deletes the array and returns RAM to OS.
    table.drop()
    
    // 4. Wait for the detached background thread to run its wipeout sequence
    Thread.sleep(2000) 
    
    // 5. Force one final GC in the main thread to ensure the JVM state is finalized
    System.gc()
    Thread.sleep(200)

    val finalJvm = getJvmUsedMB()
    val finalOs = getOsCommittedMB()
    println(f"[JVM Hoarding Test] Final    -> JVM Used: $finalJvm MB | OS Committed: $finalOs MB\n")
    
    // 6. Assertions: The JVM Heap must have plummeted!
    finalJvm should be < peakJvm
    
    // The JVM must have returned that uncommitted heap back to the Windows OS Task Manager
    if (finalOs != -1L && peakOs != -1L) {
      finalOs should be < peakOs
    }
  }
}