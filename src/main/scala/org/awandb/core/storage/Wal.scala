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

import java.io.{BufferedOutputStream, DataOutputStream, File, FileOutputStream, FileInputStream, DataInputStream}
import java.nio.ByteBuffer

class Wal(directory: String) {
  
  private val WAL_FILE = s"$directory/segment_0.wal"
  
  // Ensure directory exists
  new File(directory).mkdirs()

  // [FIX] Use vars so we can reset streams after a flush/clear
  private var fileOutput: FileOutputStream = _
  private var bufferedOutput: BufferedOutputStream = _
  private var dataOutput: DataOutputStream = _

  // Initialize streams on startup
  initStreams()

  /**
   * Helper to open/re-open streams.
   * Called on startup and after clear().
   */
  private def initStreams(): Unit = {
    // Open Append-Only Stream
    // We use a large buffer (64KB) to avoid hitting the disk for every single integer.
    fileOutput = new FileOutputStream(WAL_FILE, true)
    bufferedOutput = new BufferedOutputStream(fileOutput, 64 * 1024) 
    dataOutput = new DataOutputStream(bufferedOutput)
  }

  /**
   * Append an INSERT command to the log.
   * Format: [OpCode: Byte] [Value: Int]
   */
  def logInsert(value: Int): Unit = {
    // OpCode 1 = INSERT
    dataOutput.writeByte(1) 
    dataOutput.writeInt(value)
  }

  /**
   * [WRITE FUSION]
   * Writes a batch of integers as a single binary block.
   */
  def logBatch(values: Array[Int]): Unit = {
    if (values.isEmpty) return

    // 1 Byte (OpCode) + 4 Bytes (Int) = 5 Bytes per row
    val totalBytes = values.length * 5
    val buffer = ByteBuffer.allocate(totalBytes)

    var i = 0
    while (i < values.length) {
      buffer.put(1.toByte)      // OpCode
      buffer.putInt(values(i))  // Value
      i += 1
    }

    // Write the entire block to the stream in one go
    dataOutput.write(buffer.array())
  }

  /**
   * Force data to Disk (fsync).
   */
  def sync(): Unit = {
    dataOutput.flush()
    fileOutput.getFD.sync() // The heavy operation: Forces SSD to write
  }

  /**
   * Recover data from the log after a crash.
   */
  def recover(): Seq[Int] = {
    val file = new File(WAL_FILE)
    if (!file.exists()) return Seq.empty

    val result = scala.collection.mutable.ArrayBuffer[Int]()
    
    // Safety check: recover() creates its own read stream, independent of the write streams
    var input: DataInputStream = null
    try {
      input = new DataInputStream(new FileInputStream(file))
      while (input.available() > 0) {
        val opCode = input.readByte()
        if (opCode == 1) { // INSERT
          val value = input.readInt()
          result.append(value)
        }
      }
    } catch {
      case e: java.io.EOFException => // End of file reached
    } finally {
      if (input != null) input.close()
    }
    result.toSeq
  }

  def close(): Unit = {
    if (dataOutput != null) {
        dataOutput.close()
    }
  }
  
  /**
   * [FIXED] Clear logs and RE-OPEN streams.
   * Used by AwanTable.flush() to rotate the log after persistence.
   */
  def clear(): Unit = {
    close() // 1. Close existing stream to release file lock
    new File(WAL_FILE).delete() // 2. Delete the file
    initStreams() // 3. Re-open streams so new writes can happen
  }
}