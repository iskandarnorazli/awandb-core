/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.jni

import java.io.File

// -----------------------------------------------------------
// 1. THE JNI CLASS (Defines the native interface)
// -----------------------------------------------------------
class NativeBridge {
  
  // --- MEMORY MANAGEMENT ---
  @native def allocMainStoreNative(size: Long): Long
  @native def freeMainStoreNative(ptr: Long): Unit

  // --- IO / DATA TRANSFER ---
  @native def loadDataNative(ptr: Long, data: Array[Int]): Unit
  @native def copyToScalaNative(srcPtr: Long, dstArray: Array[Int], len: Int): Unit
  
  // [NEW] String Ingestion Native Definition
  @native def loadStringDataNative(blockPtr: Long, colIdx: Int, data: Array[String]): Unit

  // --- COMPUTE ENGINES (Legacy/Direct Pointers) ---
  @native def batchRead(colPtr: Long, indicesPtr: Long, count: Int, outDataPtr: Long): Unit
  @native def avxScanIndicesNative(colPtr: Long, rows: Int, threshold: Int, outIndices: Long): Int
  @native def avxScanIndicesMultiNative(colPtr: Long, rows: Int, thresholds: Array[Int], outCounts: Array[Int]): Unit

  // --- SMART ENGINES (Predicate Pushdown) ---
  @native def avxScanBlockNative(blockPtr: Long, colIdx: Int, threshold: Int, outIndicesPtr: Long): Int
  @native def avxScanMultiBlockNative(blockPtr: Long, colIdx: Int, thresholds: Array[Int], outCounts: Array[Int]): Unit
  
  // [NEW] German String Native Definition
  @native def avxScanStringNative(blockPtr: Long, colIdx: Int, search: String, outIndicesPtr: Long): Int

  // --- RAM ENGINES (Direct Array Access) ---
  @native def avxScanArrayNative(data: Array[Int], threshold: Int): Int
  @native def avxScanArrayMultiNative(data: Array[Int], thresholds: Array[Int], outCounts: Array[Int]): Unit

  // --- CUCKOO FILTER ---
  @native def cuckooCreateNative(capacity: Int): Long
  @native def cuckooDestroyNative(ptr: Long): Unit
  @native def cuckooInsertNative(ptr: Long, key: Int): Boolean
  @native def cuckooContainsNative(ptr: Long, key: Int): Boolean
  @native def cuckooBuildBatchNative(ptr: Long, data: Array[Int]): Unit

  // --- PERSISTENCE ---
  @native def saveColumn(ptr: Long, size: Long, path: String): Boolean
  @native def loadColumn(ptr: Long, size: Long, path: String): Boolean

  // --- BLOCK MANAGEMENT ---
  @native def createBlockNative(rowCount: Int, colCount: Int): Long
  
  @native def getColumnPtr(blockPtr: Long, colIdx: Int): Long
  @native def getBlockSize(blockPtr: Long): Long
  @native def getRowCount(blockPtr: Long): Int
  @native def loadBlockFromFile(path: String): Long

  // --- HARDWARE TOPOLOGY ---
  @native def initHardwareTopology(): Unit

  // --- OPTIMIZATION (Metadata Access) ---
  @native def getZoneMapNative(blockPtr: Long, colIdx: Int, outMinMax: Array[Int]): Unit

  @native def cuckooSaveNative(ptr: Long, path: String): Boolean
  @native def cuckooLoadNative(path: String): Long

  // --- Vector ---
  @native def loadVectorDataNative(blockPtr: Long, colIdx: Int, data: Array[Float], dim: Int): Unit
  @native def avxScanVectorCosineNative(blockPtr: Long, colIdx: Int, query: Array[Float], threshold: Float, outIndicesPtr: Long): Int
  @native def avxHashVectorNative(blockPtr: Long, colIdx: Int, outHashPtr: Long): Unit
  @native def copyToScalaLongNative(srcPtr: Long, dst: Array[Long], len: Int): Unit
}

// -----------------------------------------------------------
// 2. THE COMPANION OBJECT (The Public API)
// -----------------------------------------------------------
object NativeBridge {
  
  // =========================================================
  // DYNAMIC LIBRARY LOADING
  // =========================================================
  private val loadedLibrary: String = {
    try {
      System.loadLibrary("awan_engine_ent")
      println("[NativeBridge] MODE: Enterprise Edition (Hardware Aware & Secured)")
      "ENT"
    } catch {
      case _: UnsatisfiedLinkError =>
        try {
          System.loadLibrary("awan_engine_core")
          println("[NativeBridge] MODE: Open Source Core (Standard Engine)")
          "OSS"
        } catch {
          case e: UnsatisfiedLinkError =>
            println("!! CRITICAL ERROR !! Could not load 'awan_engine_ent' OR 'awan_engine_core'.")
            println("Ensure .dll/.so files are in java.library.path.")
            throw e
        }
    }
  }

  private val instance = new NativeBridge()

  def init(): Unit = {
    if (loadedLibrary == "ENT") {
      try {
        instance.initHardwareTopology()
        println("[NativeBridge] Hardware Topology Initialized.")
      } catch {
        case e: UnsatisfiedLinkError =>
          println(s"[NativeBridge] Warning: Enterprise lib symbol missing: ${e.getMessage}")
      }
    }
  }

  // =========================================================
  // PUBLIC API WRAPPERS
  // =========================================================

  // --- Memory ---
  def allocMainStore(size: Long): Long = {
    val ptr = instance.allocMainStoreNative(size)
    if (ptr == 0) throw new OutOfMemoryError(s"Native Alloc Failed: $size ints")
    ptr
  }
  
  def freeMainStore(ptr: Long): Unit = instance.freeMainStoreNative(ptr)
  def loadData(ptr: Long, data: Array[Int]): Unit = instance.loadDataNative(ptr, data)
  def copyToScala(srcPtr: Long, dst: Array[Int], len: Int): Unit = instance.copyToScalaNative(srcPtr, dst, len)
  
  // [NEW] String Public Wrapper (Fixes error)
  def loadStringData(blockPtr: Long, colIdx: Int, data: Array[String]): Unit = {
      instance.loadStringDataNative(blockPtr, colIdx, data)
  }

  // --- Legacy Compute ---
  def batchRead(colPtr: Long, idx: Long, count: Int, out: Long): Unit = instance.batchRead(colPtr, idx, count, out)
  def avxScanIndices(colPtr: Long, size: Long, thresh: Int, out: Long): Int = instance.avxScanIndicesNative(colPtr, size.toInt, thresh, out) 
  def avxScanIndicesMulti(colPtr: Long, size: Long, thresholds: Array[Int], outCounts: Array[Int]): Unit = instance.avxScanIndicesMultiNative(colPtr, size.toInt, thresholds, outCounts)

  // --- Smart Engines (Disk) ---
  def avxScanBlock(blockPtr: Long, colIdx: Int, threshold: Int, outIndicesPtr: Long): Int = instance.avxScanBlockNative(blockPtr, colIdx, threshold, outIndicesPtr)
  def avxScanMultiBlock(blockPtr: Long, colIdx: Int, thresholds: Array[Int], outCounts: Array[Int]): Unit = instance.avxScanMultiBlockNative(blockPtr, colIdx, thresholds, outCounts)

  // [NEW] String Search Public Wrapper (Fixes error)
  def avxScanString(blockPtr: Long, colIdx: Int, search: String): Int = {
      instance.avxScanStringNative(blockPtr, colIdx, search, 0)
  }

  // --- RAM Engines ---
  def avxScanArray(data: Array[Int], threshold: Int): Int = instance.avxScanArrayNative(data, threshold)
  def avxScanArrayMulti(data: Array[Int], thresholds: Array[Int], outCounts: Array[Int]): Unit = instance.avxScanArrayMultiNative(data, thresholds, outCounts)

  // --- Cuckoo Filter ---
  def cuckooCreate(capacity: Int): Long = instance.cuckooCreateNative(capacity)
  def cuckooDestroy(ptr: Long): Unit = instance.cuckooDestroyNative(ptr)
  def cuckooInsert(ptr: Long, key: Int): Boolean = instance.cuckooInsertNative(ptr, key)
  def cuckooContains(ptr: Long, key: Int): Boolean = instance.cuckooContainsNative(ptr, key)
  def cuckooBuildBatch(ptr: Long, data: Array[Int]): Unit = instance.cuckooBuildBatchNative(ptr, data)

  // --- Persistence & Blocks ---
  def saveColumn(ptr: Long, size: Long, path: String): Boolean = instance.saveColumn(ptr, size, path)
  def loadColumn(ptr: Long, size: Long, path: String): Boolean = instance.loadColumn(ptr, size, path)
  
  def createBlock(rowCount: Int, colCount: Int): Long = {
    val ptr = instance.createBlockNative(rowCount, colCount)
    if (ptr == 0) throw new OutOfMemoryError("Native Block Alloc Failed")
    ptr
  }
  
  def getColumnPtr(blockPtr: Long, colIdx: Int): Long = instance.getColumnPtr(blockPtr, colIdx)
  def getBlockSize(blockPtr: Long): Long = instance.getBlockSize(blockPtr)
  def getRowCount(blockPtr: Long): Int = instance.getRowCount(blockPtr)
  def loadBlockFromFile(path: String): Long = {
    val ptr = instance.loadBlockFromFile(path)
    if (ptr == 0) throw new RuntimeException(s"Failed to load block: $path")
    ptr
  }

  // --- Metadata ---
  def getZoneMap(blockPtr: Long, colIdx: Int): (Int, Int) = {
    val stats = new Array[Int](2)
    instance.getZoneMapNative(blockPtr, colIdx, stats)
    (stats(0), stats(1))
  }
  
  // --- Cuckoo Persistence ---
  def cuckooSave(ptr: Long, path: String): Boolean = instance.cuckooSaveNative(ptr, path)
  def cuckooLoad(path: String): Long = instance.cuckooLoadNative(path)

  // --- Vector ---
  def loadVectorData(blockPtr: Long, colIdx: Int, data: Array[Float], dim: Int): Unit = {
    instance.loadVectorDataNative(blockPtr, colIdx, data, dim)
  }

  def avxScanVectorCosine(blockPtr: Long, colIdx: Int, query: Array[Float], threshold: Float): Int = {
    // Pass 0 for outIndicesPtr to just count (or implement retrieval logic later)
    instance.avxScanVectorCosineNative(blockPtr, colIdx, query, threshold, 0)
  }
  
  // Overload to get results
  def avxScanVectorCosine(blockPtr: Long, colIdx: Int, query: Array[Float], threshold: Float, outIndicesPtr: Long): Int = {
    instance.avxScanVectorCosineNative(blockPtr, colIdx, query, threshold, outIndicesPtr)
  }

  def computeHash(blockPtr: Long, colIdx: Int): Array[Long] = {
    val rows = getRowCount(blockPtr)
    val outArray = new Array[Long](rows)
    
    // Pin the output array so C++ can write to it directly
    // Note: We use a temporary native buffer for safety in real prod, 
    // but for this prototype, we'll alloc a native buffer, write, then copy back.
    val outPtr = allocMainStore(rows * 2) // Alloc ints (4B), so *2 for Longs (8B)
    
    instance.avxHashVectorNative(blockPtr, colIdx, outPtr)
    
    // Copy back (Need a Long copy utility, but for now we read manually or assume Int-sized hashes?)
    // Actually, let's keep it simple: We return pointers.
    // Ideally, we add 'copyLongsToScala'. For now, let's reuse 'copyToScala' treating them as Int pairs?
    // Let's implement a 'copyLongToScalaNative' quickly in C++ if needed, 
    // OR just use 'batchRead' logic.
    
    // For Phase 4 speed, let's just assume we keep the hashes in Native RAM for the GroupBy engine.
    // We return the POINTER to the hashes.
    null // Placeholder: In real usage, we return the pointer 'outPtr'
  }
  
  // DIRECT POINTER VERSION (For high speed)
  def computeHashNativePtr(blockPtr: Long, colIdx: Int): Long = {
     val rows = getRowCount(blockPtr)
     // Allocate native buffer for Hashes (8 bytes per row)
     // allocMainStore allocates 4-byte chunks. So we need rows * 2.
     val hashPtr = allocMainStore(rows * 2) 
     instance.avxHashVectorNative(blockPtr, colIdx, hashPtr)
     hashPtr
  }
  
  // Helper to read a specific hash back (for testing)
  def getHashAt(hashPtr: Long, index: Int): Long = {
     // This is slow, only for unit tests
     // We need to access memory at hashPtr + (index * 8)
     // Since we don't have a direct "getLong" JNI, we can't easily verify in Scala without adding it.
     // Let's add 'getLongNative' to engine.cpp quickly?
     0L 
  }

  def getHashes(hashPtr: Long, count: Int): Array[Long] = {
    val arr = new Array[Long](count)
    instance.copyToScalaLongNative(hashPtr, arr, count)
    arr
  }
}