/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.jni

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock

// -----------------------------------------------------------
// 1. THE JNI CLASS (Matches C++ 'Java_org_awandb_core_jni_NativeBridge_...')
// -----------------------------------------------------------
class NativeBridge {
  // This class maps 1:1 to the C++ functions.
  // Note: Some have 'Native' suffix in C++, some don't. We match C++ exactly.

  // --- MEMORY MANAGEMENT ---
  @native def allocMainStoreNative(size: Long): Long
  @native def freeMainStoreNative(ptr: Long): Unit

  // --- IO / DATA TRANSFER ---
  @native def loadDataNative(ptr: Long, data: Array[Int]): Unit
  @native def copyToScalaNative(srcPtr: Long, dstArray: Array[Int], len: Int): Unit

  // --- COMPUTE ENGINES ---
  @native def avxScanIndicesNative(
      colPtr: Long,
      rows: Int, // C++ arg name is rows
      threshold: Int,
      outIndices: Long
  ): Int
  
  @native def batchRead(
      colPtr: Long,
      indicesPtr: Long,
      count: Int,
      outDataPtr: Long
  ): Unit

  // --- PERSISTENCE ---
  @native def saveColumn(ptr: Long, size: Long, path: String): Boolean
  @native def loadColumn(ptr: Long, size: Long, path: String): Boolean

  // --- BLOCK MANAGEMENT (Structured Memory) ---
  @native def createBlockNative(rowCount: Int, colCount: Int): Long
  @native def getColumnPtr(blockPtr: Long, colIdx: Int): Long
  @native def getBlockSize(blockPtr: Long): Long
  @native def getRowCount(blockPtr: Long): Int
  @native def loadBlockFromFile(path: String): Long

  // --- QUERY FUSION KERNEL ---
  @native def avxScanIndicesMultiNative(
      colPtr: Long,
      rows: Int,
      thresholds: Array[Int],
      outCounts: Array[Int]
  ): Unit

  // ---------------------------------------------------------
  // [ENT HOOK] HARDWARE TOPOLOGY
  // ---------------------------------------------------------
  @native def initHardwareTopology(): Unit
}

// -----------------------------------------------------------
// 2. THE COMPANION OBJECT (The Global Access Point)
// -----------------------------------------------------------
object NativeBridge {
  
  // =========================================================
  // [OPEN CORE ARCHITECTURE] DYNAMIC LIBRARY LOADING
  // =========================================================
  private val loadedLibrary: String = {
    try {
      // 1. Try to load Enterprise Lib
      System.loadLibrary("awan_engine_ent")
      println("[NativeBridge] MODE: Enterprise Edition (Hardware Aware & Secured)")
      "ENT"
    } catch {
      case _: UnsatisfiedLinkError =>
        try {
          // 2. Fallback to Core Lib
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

  // Singleton Instance
  private val instance = new NativeBridge()

  /**
   * Called at startup to initialize hardware hooks.
   */
  def init(): Unit = {
    if (loadedLibrary == "ENT") {
      try {
        instance.initHardwareTopology()
        println("[NativeBridge] Hardware Topology Initialized (NUMA/Affinity Set).")
      } catch {
        case e: UnsatisfiedLinkError =>
          println(s"[NativeBridge] Warning: Enterprise lib loaded, but symbol missing: ${e.getMessage}")
      }
    }
  }

  // =========================================================
  // PUBLIC API WRAPPERS (Clean Scala Interface)
  // =========================================================

  def allocMainStore(size: Long): Long = {
    val ptr = instance.allocMainStoreNative(size)
    if (ptr == 0) throw new OutOfMemoryError(s"Native Alloc Failed: $size ints")
    ptr
  }
  
  def freeMainStore(ptr: Long): Unit = instance.freeMainStoreNative(ptr)
  
  def loadData(ptr: Long, data: Array[Int]): Unit = instance.loadDataNative(ptr, data)
  
  def copyToScala(srcPtr: Long, dst: Array[Int], len: Int): Unit =
    instance.copyToScalaNative(srcPtr, dst, len)
  
  def avxScanIndices(colPtr: Long, size: Long, thresh: Int, out: Long): Int =
    instance.avxScanIndicesNative(colPtr, size.toInt, thresh, out) // Cast size to Int for rows arg
    
  def batchRead(colPtr: Long, idx: Long, count: Int, out: Long): Unit =
    instance.batchRead(colPtr, idx, count, out)
    
  def saveColumn(ptr: Long, size: Long, path: String): Boolean =
    instance.saveColumn(ptr, size, path)
    
  def loadColumn(ptr: Long, size: Long, path: String): Boolean =
    instance.loadColumn(ptr, size, path)

  // --- Block Management ---
  def createBlock(rowCount: Int, colCount: Int): Long = {
    val ptr = instance.createBlockNative(rowCount, colCount)
    if (ptr == 0) throw new OutOfMemoryError("Native Block Alloc Failed")
    ptr
  }

  def getColumnPtr(blockPtr: Long, colIdx: Int): Long =
    instance.getColumnPtr(blockPtr, colIdx)

  def getBlockSize(blockPtr: Long): Long = instance.getBlockSize(blockPtr)
  def getRowCount(blockPtr: Long): Int = instance.getRowCount(blockPtr)
  
  def loadBlockFromFile(path: String): Long = {
    val ptr = instance.loadBlockFromFile(path)
    if (ptr == 0) throw new RuntimeException(s"Failed to load block: $path")
    ptr
  }

  def avxScanIndicesMulti(colPtr: Long, size: Long, thresholds: Array[Int], outCounts: Array[Int]): Unit =
    instance.avxScanIndicesMultiNative(colPtr, size.toInt, thresholds, outCounts)
}