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

/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb

import org.awandb.core.jni.NativeBridge 
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, FileInputStream}
import java.nio.{ByteBuffer, ByteOrder}
import org.awandb.core.sql.SQLHandler

class PersistenceSpec extends AnyFlatSpec with Matchers {

  val TEST_DIR = new File("data/persistence_spec_v3").getAbsolutePath
  val BLOCK_FILE = new File(TEST_DIR, "mixed_block.udb").getAbsolutePath

  def cleanUp(): Unit = {
    SQLHandler.tables.clear()
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  "AwanDB Storage" should "correctly persist Mixed Schema (Int + String)" in {
    cleanUp()
    new File(TEST_DIR).mkdirs()

    val rowCount = 5
    val intData = Array(10, 20, 30, 40, 50)
    val strData = Array("apple", "banana", "cherry", "date", "elderberry")
    
    // 1. ALLOCATE & POPULATE
    // Create block with 2 columns
    val blockPtr = NativeBridge.createBlock(rowCount, 2)
    
    // Col 0: Int
    val col0 = NativeBridge.getColumnPtr(blockPtr, 0)
    NativeBridge.loadData(col0, intData)
    
    // Col 1: String
    NativeBridge.loadStringData(blockPtr, 1, strData)
    
    // 2. SAVE TO DISK
    val size = NativeBridge.getBlockSize(blockPtr)
    val success = NativeBridge.saveColumn(blockPtr, size, BLOCK_FILE) 
    success shouldBe true
    
    NativeBridge.freeMainStore(blockPtr) // Clear RAM

    // 3. BINARY INSPECTION (Basic Sanity Only)
    // We verify the Block Header to ensure the file isn't garbage.
    // We SKIP deep offset math for Strings because C++ internal layout varies.
    val file = new File(BLOCK_FILE)
    file.exists() shouldBe true
    file.length() should be > 192L // At least Headers (64*3) exist
    
    val channel = new FileInputStream(file).getChannel
    val buffer = ByteBuffer.allocate(file.length().toInt)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    channel.read(buffer)
    buffer.flip()

    // --- CHECK BLOCK HEADER ---
    buffer.position(0)
    val magic = buffer.getInt
    val version = buffer.getInt
    val savedRows = buffer.getInt 
    val savedCols = buffer.getInt
    
    magic shouldBe 0x4E415741 
    savedRows shouldBe rowCount
    savedCols shouldBe 2

    // --- CHECK COL 0 (INT) HEADER ---
    buffer.position(64)
    val c0_id = buffer.getInt
    val c0_type = buffer.getInt
    c0_id shouldBe 0
    c0_type shouldBe 0 // INT

    // --- CHECK COL 1 (STRING) HEADER ---
    buffer.position(128)
    val c1_id = buffer.getInt
    val c1_type = buffer.getInt
    c1_id shouldBe 1
    c1_type shouldBe 2 // STRING (Native Engine Type ID)

    channel.close()
  }

  // [EXTENSIVE] The Round-Trip Test is the Source of Truth
  it should "load Mixed Data back correctly (Round Trip Integrity)" in {
    val loadedPtr = NativeBridge.loadBlockFromFile(BLOCK_FILE)
    loadedPtr should not be 0
    
    val rows = NativeBridge.getRowCount(loadedPtr)
    rows shouldBe 5
    
    // A. VERIFY INT DATA
    val col0 = NativeBridge.getColumnPtr(loadedPtr, 0)
    val outInts = new Array[Int](5)
    NativeBridge.copyToScala(col0, outInts, 5)
    outInts shouldBe Array(10, 20, 30, 40, 50)

    // B. VERIFY STRING DATA (Scan)
    // This proves pointers/offsets were correctly reconstructed from disk.
    NativeBridge.avxScanString(loadedPtr, 1, "banana") shouldBe 1
    NativeBridge.avxScanString(loadedPtr, 1, "elderberry") shouldBe 1
    NativeBridge.avxScanString(loadedPtr, 1, "apple") shouldBe 1
    
    // Negative Case
    NativeBridge.avxScanString(loadedPtr, 1, "durian") shouldBe 0

    NativeBridge.freeMainStore(loadedPtr)
    cleanUp()
  }
}