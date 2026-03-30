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

package org.awandb.core.storage

import java.io.{File, FileOutputStream, ObjectOutputStream, FileInputStream, ObjectInputStream}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CopyOnWriteArrayList}
import scala.jdk.CollectionConverters._ 
import org.awandb.core.jni.NativeBridge
import org.awandb.core.util.UnsafeHelper

trait StorageRouter {
  def getPathForBlock(blockId: Int): String
  def getPathForFilter(blockId: Int, colIdx: Int): String
  def getPathForBitmap(blockId: Int): String 
  def getDataDir: String
  def initialize(): Unit
}

class SimpleStorageRouter(val dataDir: String) extends StorageRouter {
  override def initialize(): Unit = new File(dataDir).mkdirs()
  override def getDataDir: String = dataDir
  override def getPathForBlock(blockId: Int): String = f"$dataDir/block_$blockId%05d.udb"
  override def getPathForFilter(blockId: Int, colIdx: Int): String = f"$dataDir/block_$blockId%05d_col$colIdx.cuckoo"
  override def getPathForBitmap(blockId: Int): String = f"$dataDir/block_$blockId%05d.del" 
}

// ==================================================================================
// THREAD-SAFE BLOCK MANAGER (Lazy Indexing + Deletion Vectors)
// ==================================================================================

class BlockManager(router: StorageRouter, val enableIndex: Boolean) {
  
  def this(dataDir: String, enableIndex: Boolean = true) = 
    this(new SimpleStorageRouter(dataDir), enableIndex)

  private val blockCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  
  private val loadedBlocks = new CopyOnWriteArrayList[Long]() 
  // Maps blockIdx -> Array of Cuckoo Pointers (one per column)
  private val loadedFilters = new ConcurrentHashMap[Int, Array[Long]]() 

  // Maps blockIdx -> Array of (Min, Max) tuples (one per column)
  private val loadedZoneMaps = new ConcurrentHashMap[Int, Array[(Int, Int)]]()
  private val pendingIndexes = new ConcurrentLinkedQueue[java.lang.Integer]()
  private val deletionBitmaps = new ConcurrentHashMap[Long, java.util.BitSet]()

  // [NEW] Cache for Native Bitmask Pointers and Dirty Flags
  private val nativeBitmaps = new ConcurrentHashMap[Long, java.lang.Long]()
  private val bitmapDirty = new ConcurrentHashMap[Long, java.lang.Boolean]()

  router.initialize()

  /**
   * RECOVERY: Loads blocks, filters, and [NEW] deletion bitmaps.
   */
  def recover(): Unit = {
    val dir = new File(router.getDataDir)
    if (!dir.exists() || !dir.isDirectory) return

    loadedBlocks.clear()
    loadedFilters.clear()
    deletionBitmaps.clear()

    val files = dir.listFiles().filter(f => f.isFile && f.getName.endsWith(".udb") && f.getName.startsWith("block_"))
    val sortedFiles = files.sortBy(_.getName)

    println(s"[BlockManager] Recovering ${sortedFiles.length} blocks...")

    var maxId = -1
    for (file <- sortedFiles) {
      val ptr = NativeBridge.loadBlockFromFile(file.getAbsolutePath)
      if (ptr != 0) {
          loadedBlocks.add(ptr)
          val blockIdx = loadedBlocks.size() - 1
          
          // Load Deletions for this block immediately
          loadBitmap(blockIdx, ptr)

          val name = file.getName
          val idPart = name.stripPrefix("block_").stripSuffix(".udb")
          val id = scala.util.Try(idPart.toInt).getOrElse(0)
          if (id > maxId) maxId = id
          
          if (enableIndex) {
              // We defer loading to the daemon so it can use the isVector flags
              pendingIndexes.offer(blockIdx)
          }
      }
    }
    blockCounter.set(maxId + 1)
    println(s"[BlockManager] Recovery Complete. Next Block ID: ${blockCounter.get()}")
  }

  /**
   * [FAST PATH] Create and Persist Block Data ONLY.
   */
  def createAndPersistBlock(columnsData: Seq[Any]): Unit = {
    if (columnsData.isEmpty) return

    // Determine base row count from the first column
    val rowCount = columnsData.head match {
      case a: Array[Int] => a.length
      case a: Array[String] => a.length
      case a: Array[Float] => a.length / 128 // Assumes 128-dim vector fallback
      case _ => 0
    }

    if (rowCount == 0) return
    val colCount = columnsData.length
    val currentId = blockCounter.getAndIncrement()
    
    // [FIX] Calculate exact memory allocation in bytes for each column
    val colSizesBytes = new Array[Int](colCount)
    
    for (i <- 0 until colCount) {
      columnsData(i) match {
        case ints: Array[Int] =>
          colSizesBytes(i) = rowCount * 4
          
        case floats: Array[Float] =>
          colSizesBytes(i) = floats.length * 4
          
        case strs: Array[String] =>
          var stringPoolBytes = 0
          for (s <- strs) {
            if (s != null) {
              val byteLen = s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
              if (byteLen > 12) {
                stringPoolBytes += byteLen
              }
            }
          }
          // 16 bytes per GermanString Header + String Pool
          colSizesBytes(i) = (rowCount * 16) + stringPoolBytes
          
        case _ => 
          colSizesBytes(i) = 0
      }
    }

    // Allocate with the exact calculated capacity and the TRUE row count
    val blockPtr = NativeBridge.createBlock(rowCount, colCount, colSizesBytes)
    
    try {
        val dataArray = columnsData.toArray
        
        for (colIdx <- 0 until colCount) {
           dataArray(colIdx) match {
               case ints: Array[Int] =>
                   val colPtr = org.awandb.core.jni.NativeBridge.getColumnPtr(blockPtr, colIdx)
                   org.awandb.core.jni.NativeBridge.loadData(colPtr, ints)
               case strs: Array[String] =>
                   org.awandb.core.jni.NativeBridge.loadStringData(blockPtr, colIdx, strs)
               case floats: Array[Float] =>
                   // Dynamically calculate dimension based on total rows
                   val dim = floats.length / rowCount 
                   org.awandb.core.jni.NativeBridge.loadVectorData(blockPtr, colIdx, floats, dim)
               case _ => 
                   throw new IllegalArgumentException(s"Unsupported column type at index $colIdx")
           }
        }
        
        val filename = router.getPathForBlock(currentId)
        val saved = NativeBridge.saveColumn(blockPtr, NativeBridge.getBlockSize(blockPtr), filename) 
        
        if (!saved) {
          throw new RuntimeException(s"Failed to save block: $filename")
        }

        loadedBlocks.add(blockPtr)
        
        if (enableIndex && rowCount > 0) {
            pendingIndexes.offer(loadedBlocks.size() - 1)
        }

    } catch {
        case e: Throwable =>
            // [CRITICAL FIX] Use destroyBlock instead of freeMainStore to prevent native heap corruption
            NativeBridge.destroyBlock(blockPtr) 
            throw e
    }
  }

  /**
   * [TRUE ZERO-COPY] Create and Persist Block directly from Off-Heap Pointers.
   * Bypasses the JVM Heap entirely. Supports Int, String, and Vector.
   */
  def createAndPersistBlockFromPointers(
      rowCount: Int, 
      colTypes: Array[Int], 
      dataPointers: Array[Long],
      offsetPointers: Array[Long],
      colSizesBytes: Array[Int],
      vectorDims: Array[Int]
  ): Long = {
    if (rowCount == 0) return 0L
    val colCount = colTypes.length

    // 1. Allocate the C++ Block natively (Memory sizing is dynamically calculated by the Network Layer)
    val blockPtr = NativeBridge.createBlock(rowCount, colCount, colSizesBytes)

    try {
      // 2. Blast memory from Arrow directly into the C++ Column offsets!
      for (colIdx <- 0 until colCount) {
        val colType = colTypes(colIdx)
        val destPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
        
        colType match {
            case 0 => // INT (Direct Memcpy)
                NativeBridge.memcpy(dataPointers(colIdx), destPtr, colSizesBytes(colIdx))
                
            case 2 => // STRING (Offset Buffer + Data Buffer Translation)
                NativeBridge.bulkLoadArrowStrings(blockPtr, colIdx, offsetPointers(colIdx), dataPointers(colIdx), rowCount)
                
            case 3 => // VECTOR (Set Dimension Metadata, then Direct Memcpy)
                NativeBridge.setVectorDim(blockPtr, colIdx, vectorDims(colIdx))
                NativeBridge.memcpy(dataPointers(colIdx), destPtr, colSizesBytes(colIdx))
                
            case _ => 
                throw new UnsupportedOperationException(s"Unsupported colType $colType for True Zero-Copy Bulk Load.")
        }
      }

      // 3. Save to disk (This implicitly computes Min/Max Zone Maps via C++)
      val currentId = blockCounter.getAndIncrement()
      val filename = router.getPathForBlock(currentId)
      val saved = NativeBridge.saveColumn(blockPtr, NativeBridge.getBlockSize(blockPtr), filename)

      if (!saved) throw new RuntimeException(s"Failed to save block: $filename")

      loadedBlocks.add(blockPtr)

      // Queue for Cuckoo Filter generation
      if (enableIndex && rowCount > 0) {
        pendingIndexes.offer(loadedBlocks.size() - 1)
      }

      blockPtr
    } catch {
      case e: Throwable =>
        NativeBridge.destroyBlock(blockPtr)
        throw e
    }
  }

  // Schema-Aware Index Builder
  def buildPendingIndexes(isVectorFlags: Array[Boolean]): Int = {
    val blockIdx = pendingIndexes.poll()
    if (blockIdx == null) return 0
    if (blockIdx >= loadedBlocks.size()) return 0 
    if (loadedFilters.containsKey(blockIdx)) return 0 

    val blockPtr = loadedBlocks.get(blockIdx)
    val rowCount = NativeBridge.getRowCount(blockPtr)
    if (rowCount == 0) return 0

    val colCount = isVectorFlags.length
    val filterArray = new Array[Long](colCount)
    val zoneMapArray = new Array[(Int, Int)](colCount)

    for (colIdx <- 0 until colCount) {
      if (!isVectorFlags(colIdx)) {
        // 1. Fetch native column data
        val colPtr = NativeBridge.getColumnPtr(blockPtr, colIdx)
        val data = new Array[Int](rowCount)
        NativeBridge.copyToScala(colPtr, data, rowCount)

        // 2. Build Cuckoo Filter for this specific column
        val filterPtr = NativeBridge.cuckooCreate(math.max((rowCount * 1.5).toInt, 1024))
        NativeBridge.cuckooBuildBatch(filterPtr, data)
        filterArray(colIdx) = filterPtr

        // 3. Save Cuckoo Filter 
        val path = router.getPathForFilter(blockIdx, colIdx) 
        NativeBridge.cuckooSave(filterPtr, path)

        // 4. Fetch and cache Zone Map natively
        zoneMapArray(colIdx) = NativeBridge.getZoneMap(blockPtr, colIdx)
      } else {
        // Vectors skip Cuckoo Filters and standard Min/Max Maps
        filterArray(colIdx) = 0L
        zoneMapArray(colIdx) = (0, 0)
      }
    }

    loadedFilters.put(blockIdx, filterArray)
    loadedZoneMaps.put(blockIdx, zoneMapArray)
    
    1 
  }

  def getLoadedBlocks: scala.collection.Seq[Long] = loadedBlocks.asScala.toSeq

  //  Retrieve O(1) Zone Map bounds for a specific block and column
  def getZoneMap(blockIdx: Int, colIdx: Int): (Int, Int) = {
    val maps = loadedZoneMaps.get(blockIdx)
    if (maps != null && colIdx < maps.length) {
      maps(colIdx)
    } else {
      // Fallback if not loaded/indexed yet
      (Int.MinValue, Int.MaxValue) 
    }
  }

  // Multi-Column Cuckoo Filter check
  def mightContain(blockIdx: Int, colIdx: Int, key: Int): Boolean = {
    if (!enableIndex) return true
    
    val filterArray = loadedFilters.get(blockIdx)
    if (filterArray == null || colIdx >= filterArray.length) return true 
    
    val ptr = filterArray(colIdx)
    if (ptr == 0L) return true 
    
    NativeBridge.cuckooContains(ptr, key)
  }

  // Safely retrieve the full array of Cuckoo Filters for Compaction
  def getFilterArray(blockIdx: Int): Array[Long] = {
    loadedFilters.get(blockIdx)
  }

  def saveBitmaps(): Unit = {
    val it = loadedBlocks.iterator()
    var idx = 0
    while(it.hasNext) {
      val ptr = it.next()
      val bitset = deletionBitmaps.get(ptr)
      
      if (bitset != null && !bitset.isEmpty) {
         val path = router.getPathForBitmap(idx)
         try {
           val fos = new FileOutputStream(path)
           val oos = new ObjectOutputStream(fos)
           oos.writeObject(bitset)
           oos.close()
         } catch {
           case e: Exception => println(s"[BlockManager] Failed to save bitmap $idx: ${e.getMessage}")
         }
      }
      idx += 1
    }
  }

  private def loadBitmap(blockIdx: Int, blockPtr: Long): Unit = {
    val path = router.getPathForBitmap(blockIdx)
    val file = new File(path)
    if (file.exists()) {
       try {
         val fis = new FileInputStream(file)
         val ois = new ObjectInputStream(fis)
         val bitset = ois.readObject().asInstanceOf[java.util.BitSet]
         deletionBitmaps.put(blockPtr, bitset)
         bitmapDirty.put(blockPtr, true) // [NEW] Flag for native sync
         ois.close()
         println(s"[BlockManager] Loaded Deletions for Block $blockIdx")
       } catch {
         case e: Exception => println(s"[BlockManager] Failed to load bitmap: ${e.getMessage}")
       }
    }
  }

  def markDeleted(blockPtr: Long, rowId: Int): Unit = {
    deletionBitmaps.computeIfAbsent(blockPtr, _ => new java.util.BitSet()).set(rowId)
    bitmapDirty.put(blockPtr, true) // [NEW] Flag for native sync
  }

  def getDeletionBitSet(blockPtr: Long): java.util.BitSet = {
    deletionBitmaps.get(blockPtr)
  }

  def isClean(blockPtr: Long): Boolean = {
    val bitset = deletionBitmaps.get(blockPtr)
    bitset == null || bitset.isEmpty
  }

  def isDeleted(blockPtr: Long, rowId: Int): Boolean = {
    val bits = deletionBitmaps.get(blockPtr)
    if (bits == null) false else bits.get(rowId)
  }

  /**
   * [NEW] Retrieves the cached Native Bitmask Pointer.
   * Lazily synchronizes the JVM BitSet to Native Memory only if dirty.
   */
  def getNativeDeletionBitmap(blockPtr: Long, rowCount: Int): Long = {
    val isDirty = bitmapDirty.getOrDefault(blockPtr, false)
    var ptr = nativeBitmaps.getOrDefault(blockPtr, 0L)

    // Only do work if it's dirty or hasn't been allocated yet
    if (isDirty || (ptr == 0L && deletionBitmaps.containsKey(blockPtr))) {
      
      // Synchronize to prevent multiple threads from allocating native memory for the same block
      deletionBitmaps.get(blockPtr).synchronized {
        if (bitmapDirty.getOrDefault(blockPtr, false) || nativeBitmaps.getOrDefault(blockPtr, 0L) == 0L) {
          val bitset = deletionBitmaps.get(blockPtr)
          
          if (bitset != null && !bitset.isEmpty) {
            val rawBytes = bitset.toByteArray
            val requiredBytes = (rowCount + 7) / 8
            val paddedBytes = if (rawBytes.length == requiredBytes) rawBytes else rawBytes.padTo(requiredBytes, 0.toByte)
            val intsNeeded = (requiredBytes + 3) / 4
            val paddedInts = new Array[Int](intsNeeded)
            
            java.nio.ByteBuffer.wrap(paddedBytes.padTo(intsNeeded * 4, 0.toByte))
              .order(java.nio.ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer().get(paddedInts)

            if (ptr == 0L) {
              ptr = NativeBridge.allocMainStore(intsNeeded)
              nativeBitmaps.put(blockPtr, ptr)
            }
            NativeBridge.loadData(ptr, paddedInts)
          }
          bitmapDirty.put(blockPtr, false)
        }
      }
    }
    ptr
  }

  // O(1) zero-allocation access to the raw pointer
  def getBlockPtr(idx: Int): Long = loadedBlocks.get(idx)

  // Safely swap blocks during compaction without breaking encapsulation
  def swapBlocks(oldBlocks: Array[Long], newBlockPtr: Long): Unit = {
    val removedIndices = oldBlocks.map(ptr => loadedBlocks.indexOf(ptr)).filter(_ != -1).sorted.reverse
    
    oldBlocks.foreach { ptr => 
      loadedBlocks.remove(ptr)
      deletionBitmaps.remove(ptr)
      nativeBitmaps.remove(ptr)
      bitmapDirty.remove(ptr)
    }
    
    if (newBlockPtr != 0L) {
      loadedBlocks.add(newBlockPtr)
    }

    if (enableIndex) {
      // Temporarily extract surviving 2D Arrays
      val survivingFilters = new java.util.HashMap[Int, Array[Long]]()
      val survivingZoneMaps = new java.util.HashMap[Int, Array[(Int, Int)]]()
      var currentIdx = 0
      
      val oldSize = loadedBlocks.size() + oldBlocks.length - (if (newBlockPtr != 0L) 1 else 0)
      for (i <- 0 until oldSize) {
        if (!removedIndices.contains(i)) {
          val filterArr = loadedFilters.get(i)
          val zoneMapArr = loadedZoneMaps.get(i)
          
          if (filterArr != null) survivingFilters.put(currentIdx, filterArr)
          if (zoneMapArr != null) survivingZoneMaps.put(currentIdx, zoneMapArr)
          
          currentIdx += 1
        } else {
          // Old arrays retired by AwanTable
          loadedFilters.remove(i)
          loadedZoneMaps.remove(i)
        }
      }
      
      loadedFilters.clear()
      loadedFilters.putAll(survivingFilters)
      
      loadedZoneMaps.clear()
      loadedZoneMaps.putAll(survivingZoneMaps)
      
      if (newBlockPtr != 0L) {
         pendingIndexes.offer(loadedBlocks.size() - 1)
      }
    }
  }

  /**
   * [NEW] Instantly frees all native C++ memory without flushing or saving metadata.
   * Required for ZERO-LEAK DDL (DROP TABLE) to prevent JVM memory GC deadlocks.
   */
  def dropAllBlocksInstantly(): Unit = {
    // 1. Destroy the physical C++ block memory instantly
    loadedBlocks.asScala.foreach { ptr =>
      NativeBridge.destroyBlock(ptr)
    }

    // 2. Free native deletion bitmasks directly from the values
    nativeBitmaps.values().asScala.foreach { bitmaskPtr =>
      if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
    }

    // 3. Destroy all associated Cuckoo Filters in native memory safely 
    // regardless of map keys (Fixes the `idx` pointer leak)
    loadedFilters.values().asScala.foreach { filterArray =>
      if (filterArray != null) {
        filterArray.foreach { filterPtr =>
          if (filterPtr != 0L) NativeBridge.cuckooDestroy(filterPtr)
        }
      }
    }

    // 4. Clear all JVM references so GC can collect the Scala wrapper objects
    loadedBlocks.clear()
    loadedFilters.clear()
    loadedZoneMaps.clear()
    deletionBitmaps.clear()
    nativeBitmaps.clear()
    bitmapDirty.clear()
    pendingIndexes.clear()
  }

  def close(): Unit = {
    // 1. Destroy the physical C++ block memory
    loadedBlocks.asScala.foreach { ptr =>
      // [CRITICAL FIX] Invoke the C++ destructor to free the massive internal payloads!
      NativeBridge.destroyBlock(ptr)
    }

    // 2. Free native deletion bitmasks directly from the map values
    nativeBitmaps.values().asScala.foreach { bitmaskPtr =>
      if (bitmaskPtr != 0L) NativeBridge.freeMainStore(bitmaskPtr)
    }

    // 3. Destroy all associated Cuckoo Filters in native memory safely 
    // regardless of map keys (Fixes the `idx` pointer leak)
    loadedFilters.values().asScala.foreach { filterArray =>
      if (filterArray != null) {
        filterArray.foreach { filterPtr =>
          if (filterPtr != 0L) NativeBridge.cuckooDestroy(filterPtr)
        }
      }
    }

    // 4. Clear all JVM references so GC can collect the Scala wrapper objects
    loadedBlocks.clear()
    loadedFilters.clear()
    loadedZoneMaps.clear()
    deletionBitmaps.clear()
    nativeBitmaps.clear()
    bitmapDirty.clear()
    // Included pendingIndexes just to be completely thorough
    pendingIndexes.clear() 
  }
}