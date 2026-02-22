/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.core.storage

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.awandb.core.jni.NativeBridge

// Wrapper for Block Pointers
case class Block(ptr: Long, rowCount: Int)

class NativeColumn(val name: String, val isString: Boolean = false, val useDictionary: Boolean = false) {
  
  // 1. Delta Store (Write-Optimized RAM)
  val deltaIntBuffer = new ArrayBuffer[Int]()
  val deltaStringBuffer = new ArrayBuffer[String]()

  // 2. Snapshot Store (Read-Optimized Disk/Native RAM)
  val snapshotBlocks = new ListBuffer[Block]()

  // 3. Dictionary State (Native C++)
  // We allocate this lazily on first insert or recovery if enabled
  var dictionaryPtr: Long = 0

  if (useDictionary && isString) {
    dictionaryPtr = NativeBridge.dictionaryCreate()
  }

  // -----------------------------------------------------------
  // INSERT API
  // -----------------------------------------------------------

  def insert(value: Int): Unit = {
    if (isString) throw new IllegalStateException(s"Col $name is String")
    deltaIntBuffer.append(value)
  }

  def insert(value: String): Unit = {
    if (!isString) throw new IllegalStateException(s"Col $name is Int")
    deltaStringBuffer.append(value)
  }

  def insertBatch(values: Array[Int]): Unit = {
    if (isString) throw new IllegalStateException(s"Col $name is String")
    deltaIntBuffer ++= values
  }

  def insertBatch(values: Array[String]): Unit = {
    if (!isString) throw new IllegalStateException(s"Col $name is Int")
    deltaStringBuffer ++= values
  }

  // -----------------------------------------------------------
  // ENCODING LOGIC (The Magic)
  // -----------------------------------------------------------

  /**
   * Compresses the current String buffer into Integer IDs.
   * Uses the C++ Native Dictionary for extreme speed.
   */
  def encodeDelta(): Array[Int] = {
    if (!useDictionary || dictionaryPtr == 0) return Array.empty
    if (deltaStringBuffer.isEmpty) return Array.empty

    val count = deltaStringBuffer.length
    val outIds = new Array[Int](count)
    
    // 1. Alloc Native Buffer for results (Reuse NativeBridge aligned alloc)
    val nativeIdsPtr = NativeBridge.allocMainStore(count)
    
    try {
      // 2. Batch Encode (C++ does the heavy lifting: String -> Hash -> ID)
      NativeBridge.dictionaryEncodeBatch(dictionaryPtr, deltaStringBuffer.toArray, nativeIdsPtr)
      
      // 3. Copy back to Scala Array (for Block creation)
      NativeBridge.copyToScala(nativeIdsPtr, outIds, count)
    } finally {
      NativeBridge.freeMainStore(nativeIdsPtr)
    }
    
    outIds
  }

  // -----------------------------------------------------------
  // QUERY HELPER (Thread-Safe Dictionary Access)
  // -----------------------------------------------------------
  
  def getDictId(value: String): Int = this.synchronized {
    if (dictionaryPtr == 0L) dictionaryPtr = NativeBridge.dictionaryCreate()
    NativeBridge.dictionaryEncode(dictionaryPtr, value)
  }

  def getDictStr(id: Int): String = this.synchronized {
    if (dictionaryPtr == 0L) return ""
    val str = NativeBridge.dictionaryDecode(dictionaryPtr, id)
    if (str == null) "" else str
  }

  // -----------------------------------------------------------
  // PERSISTENCE (Native C++)
  // -----------------------------------------------------------

  def saveDictionary(path: String): Unit = {
    if (useDictionary && dictionaryPtr != 0) {
      NativeBridge.dictionarySave(dictionaryPtr, path)
    }
  }

  def loadDictionary(path: String): Unit = {
    if (useDictionary) {
      // Destroy existing empty dictionary if present
      if (dictionaryPtr != 0) {
          NativeBridge.dictionaryDestroy(dictionaryPtr)
      }
      
      // Try load from disk
      dictionaryPtr = NativeBridge.dictionaryLoad(path)
      
      // If file doesn't exist, create a fresh one
      if (dictionaryPtr == 0) {
          dictionaryPtr = NativeBridge.dictionaryCreate()
      }
    }
  }

  // -----------------------------------------------------------
  // LIFECYCLE API
  // -----------------------------------------------------------

  def clearDelta(): Unit = {
    deltaIntBuffer.clear()
    deltaStringBuffer.clear()
  }
  
  def isEmpty: Boolean = {
    if (isString) deltaStringBuffer.isEmpty else deltaIntBuffer.isEmpty
  }

  def close(): Unit = {
    snapshotBlocks.clear()
    deltaIntBuffer.clear()
    deltaStringBuffer.clear()
    
    if (dictionaryPtr != 0) {
      NativeBridge.dictionaryDestroy(dictionaryPtr)
      dictionaryPtr = 0
    }
  }

  def toIntArray: Array[Int] = deltaIntBuffer.toArray
  def toStringArray: Array[String] = deltaStringBuffer.toArray
  def toArray: Array[Int] = toIntArray
}