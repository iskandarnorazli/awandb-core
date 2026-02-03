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

import java.io.{BufferedOutputStream, DataOutputStream, File, FileOutputStream, FileInputStream, DataInputStream}
import java.nio.ByteBuffer

class Wal(directory: String) {
  
  private val WAL_FILE = s"$directory/segment_0.wal"
  
  // Ensure directory exists
  new File(directory).mkdirs()

  // Open Append-Only Stream
  // We use a large buffer (64KB) to avoid hitting the disk for every single integer.
  // This is a trade-off: If we crash, we lose the last 64KB of data, but we get huge speed.
  private val fileOutput = new FileOutputStream(WAL_FILE, true)
  private val bufferedOutput = new BufferedOutputStream(fileOutput, 64 * 1024) 
  private val dataOutput = new DataOutputStream(bufferedOutput)

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
   * This reduces overhead by making 1 IO call instead of N calls.
   * * Format matches logInsert: [OpCode][Val][OpCode][Val]...
   * This ensures the existing 'recover()' method works without changes.
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
   * Call this when the user commits a transaction or periodically.
   */
  def sync(): Unit = {
    dataOutput.flush()
    fileOutput.getFD.sync() // The heavy operation: Forces SSD to write
  }

  /**
   * Recover data from the log after a crash.
   * Returns a sequence of integers that were inserted.
   */
  def recover(): Seq[Int] = {
    val file = new File(WAL_FILE)
    if (!file.exists()) return Seq.empty

    val result = scala.collection.mutable.ArrayBuffer[Int]()
    val input = new DataInputStream(new FileInputStream(file))

    try {
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
      input.close()
    }
    result.toSeq
  }

  def close(): Unit = {
    dataOutput.close()
  }
  
  // Helper to clear logs (for tests)
  def clear(): Unit = {
    close()
    new File(WAL_FILE).delete()
  }
}