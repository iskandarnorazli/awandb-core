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

package org.awandb

import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge 
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, FileInputStream}
import java.nio.{ByteBuffer, ByteOrder}

class PersistenceSpec extends AnyFlatSpec with Matchers {

  // Use Absolute Path to prevent C++ fopen failures
  val TEST_DIR = new File("data/persistence_spec_unique").getAbsolutePath
  val BLOCK_FILE = new File(TEST_DIR, "block_test.udb").getAbsolutePath

  def cleanUp(): Unit = {
    val dir = new File(TEST_DIR)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  "AwanDB Storage" should "create a Block with the correct Binary Header Format" in {
    cleanUp()
    new File(TEST_DIR).mkdirs()

    val rowCount = 100
    val data = (0 until rowCount).toArray
    
    // 1. Create Block in Unified Memory (C++)
    val blockPtr = NativeBridge.createBlock(rowCount, 1)
    
    // 2. Populate Data
    val colPtr = NativeBridge.getColumnPtr(blockPtr, 0)
    NativeBridge.loadData(colPtr, data)
    
    // 3. Save to Disk
    val size = NativeBridge.getBlockSize(blockPtr)
    NativeBridge.saveColumn(blockPtr, size, BLOCK_FILE) 
    NativeBridge.freeMainStore(blockPtr)

    // 4. VERIFY BINARY CONTRACT
    val file = new File(BLOCK_FILE)
    file.exists() shouldBe true
    
    val channel = new FileInputStream(file).getChannel
    val buffer = ByteBuffer.allocate(file.length().toInt)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    channel.read(buffer)
    buffer.flip()

    // --- A. CHECK BLOCK HEADER (64 Bytes) ---
    val magic = buffer.getInt      // 0
    val version = buffer.getInt    // 4
    val rows = buffer.getInt       // 8
    val cols = buffer.getInt       // 12

    magic shouldBe 0x4E415741 
    rows shouldBe rowCount

    // Move to start of Column Headers (Offset 64)
    buffer.position(64)

    // --- B. CHECK COLUMN HEADER (64 Bytes) ---
    // struct ColumnHeader {
    //    uint32_t col_id;          // +0
    //    uint32_t type;            // +4
    //    uint32_t compression;     // +8
    //    uint32_t stride;          // +12
    //    uint64_t data_offset;     // +16
    //    uint64_t data_length;     // +24
    //    uint64_t pool_offset;     // +32  <-- WAS MISSING
    //    uint64_t pool_length;     // +40  <-- WAS MISSING
    //    int32_t  min_int;         // +48
    //    int32_t  max_int;         // +52
    //    ... padding ...
    // }
    
    val colId = buffer.getInt        // 64
    val colType = buffer.getInt      // 68
    val compression = buffer.getInt  // 72
    val stride = buffer.getInt       // 76

    val dataOffset = buffer.getLong  // 80
    val dataLength = buffer.getLong  // 88
    
    // [FIX] Skip String Pool Fields (16 bytes)
    val poolOffset = buffer.getLong  // 96
    val poolLength = buffer.getLong  // 104
    
    // [FIX] Read Zone Maps at correct offsets
    val minInt = buffer.getInt       // 112
    val maxInt = buffer.getInt       // 116

    // Assertions
    colId shouldBe 0
    stride shouldBe 0  // 0 usually means default/dense in our current logic (or check if set to 4)
    
    // The Zone Map verification that was failing
    minInt shouldBe 0    
    maxInt shouldBe 99   

    // --- C. CHECK DATA ---
    // Data should be at 256 (Header 64 + ColHeader 64 + Padding 128)
    val expectedDataOffset = 256
    dataOffset shouldBe expectedDataOffset

    buffer.position(expectedDataOffset)
    val val0 = buffer.getInt
    val val1 = buffer.getInt
    
    val0 shouldBe 0
    val1 shouldBe 1
    
    channel.close()
  }

  it should "load a Block back from disk and verify data integrity" in {
    val loadedPtr = NativeBridge.loadBlockFromFile(BLOCK_FILE)
    loadedPtr should not be 0
    
    val rows = NativeBridge.getRowCount(loadedPtr)
    rows shouldBe 100
    
    val colPtr = NativeBridge.getColumnPtr(loadedPtr, 0)
    val outArray = new Array[Int](100)
    NativeBridge.copyToScala(colPtr, outArray, 100)
    
    outArray(0) shouldBe 0
    outArray(99) shouldBe 99
    
    NativeBridge.freeMainStore(loadedPtr)
    cleanUp()
  }
}