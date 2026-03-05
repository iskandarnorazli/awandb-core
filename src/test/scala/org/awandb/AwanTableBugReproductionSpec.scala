/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge

class AwanTableBugReproductionSpec extends AnyFunSuite with BeforeAndAfterAll {

  val testDir = "target/data_bug_repro"

  override def beforeAll(): Unit = {
    NativeBridge.init()
    val dir = new java.io.File(testDir)
    if (dir.exists()) deleteRecursively(dir)
    dir.mkdirs()
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  test("BUG 1: Primary Index Stale blockIdx Shift during Compaction") {
    // Capacity = 10 means it flushes to a new block every 10 rows
    val table = new AwanTable("index_shift_test", capacity = 10, dataDir = testDir, daemonIntervalMs = 999999L)
    table.addColumn("id", isString = false, isVector = false)
    table.addColumn("val", isString = false, isVector = false)

    println("[Test 1] Inserting 30 rows to create 3 distinct Native Blocks...")
    for (i <- 0 until 30) {
      table.insertRow(Array(i, i * 100))
      if ((i + 1) % 10 == 0) table.flush() 
    }
    // Block 0 has IDs 0-9
    // Block 1 has IDs 10-19
    // Block 2 has IDs 20-29

    println("[Test 1] Deleting rows to make Block 0 and Block 1 eligible for compaction...")
    table.delete(1)
    table.delete(11)

    println("[Test 1] Forcing Compaction...")
    // This will merge Block 0 and Block 1. Block 2 will shift from index 2 to index 1.
    table.compactor.compact(0.0)

    println("[Test 1] Attempting to read ID 25 (Originally in Block 2)...")
    // EXPECTED CRASH: The primaryIndex still thinks ID 25 is in Block 2. 
    // It will ask BlockManager for Block 2, which is either out of bounds or points to garbage memory.
    val row = table.getRow(25)
    
    // If it doesn't crash, it means it returned garbage data or None
    assert(row.isDefined, "Row 25 completely vanished from the index!")
    assert(row.get(1) == 2500, s"Data corruption! Expected 2500 but got ${row.get(1)}")
    
    table.close()
  }

  test("BUG 2: scanAll() lacks EBMM protection leading to Use-After-Free") {
    val table = new AwanTable("scan_all_test", capacity = 10, dataDir = testDir, daemonIntervalMs = 999999L)
    table.addColumn("id", isString = false, isVector = false)

    println("[Test 2] Inserting data and flushing to Disk...")
    for (i <- 0 until 10) table.insertRow(Array(i))
    table.flush()

    println("[Test 2] Initiating scanAll() lazy iterator...")
    val iterator = table.scanAll()
    
    // Read the first row to initialize the native memory read
    assert(iterator.hasNext)
    iterator.next()

    println("[Test 2] Simulating concurrent compaction and memory reclamation...")
    table.delete(1)
    table.compactor.compact(0.0) // Creates a new block, retires the old one
    
    // Advance the global epoch and try to reclaim. 
    // Because scanAll() didn't register the thread in EpochManager, this WILL free the native memory immediately.
    table.epochManager.advanceGlobalEpoch()
    table.epochManager.tryReclaim()

    println("[Test 2] Resuming iterator read...")
    // EXPECTED CRASH: The JVM will try to read the next item from the retired native block via JNI.
    // This triggers a SIGSEGV.
    if (iterator.hasNext) {
       iterator.next() 
    }
    
    table.close()
  }
}