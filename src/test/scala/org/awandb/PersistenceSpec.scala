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
import org.awandb.core.jni.NativeBridge // <--- CRITICAL IMPORT
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, FileInputStream}
import java.nio.{ByteBuffer, ByteOrder}

class PersistenceSpec extends AnyFlatSpec with Matchers {

  // [CRITICAL] Unique Directory to avoid collision with other specs
  val TEST_DIR = "data/persistence_spec_unique"
  val BLOCK_FILE = s"$TEST_DIR/block_test.udb"

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
    // This is the "Gold Standard" test that proves we own the file format.
    val file = new File(BLOCK_FILE)
    file.exists() shouldBe true
    
    val channel = new FileInputStream(file).getChannel
    val buffer = ByteBuffer.allocate(file.length().toInt)
    buffer.order(ByteOrder.LITTLE_ENDIAN) // C++ is Little Endian
    channel.read(buffer)
    buffer.flip()

    // 
    // 
    // Visualizing: Header (64B) -> Col Header (64B) -> Padding -> Data

    // --- A. CHECK BLOCK HEADER (64 Bytes) ---
    // struct BlockHeader {
    //    uint32_t magic_number;
    //    uint32_t version;
    //    uint32_t row_count;
    //    uint32_t column_count;
    //    uint64_t reserved[6];
    // }
    
    val magic = buffer.getInt      // Offset 0
    val version = buffer.getInt    // Offset 4
    val rows = buffer.getInt       // Offset 8
    val cols = buffer.getInt       // Offset 12

    magic shouldBe 0x4E415741 // "AWAN" in hex
    version shouldBe 1
    rows shouldBe rowCount
    cols shouldBe 1

    // Skip Reserved: 64 total - 16 read = 48 bytes to skip
    buffer.position(buffer.position() + 48) // Now at Offset 64

    // --- B. CHECK COLUMN HEADER (64 Bytes) ---
    // struct ColumnHeader {
    //    uint32_t col_id;
    //    uint32_t type;
    //    uint32_t compression;
    //    uint32_t padding1;       <-- Don't forget this!
    //    uint64_t data_offset;
    //    ...
    // }
    
    val colId = buffer.getInt        // Offset 64
    val colType = buffer.getInt      // Offset 68
    val compression = buffer.getInt  // Offset 72
    val padding1 = buffer.getInt     // Offset 76 (Skip)

    val dataOffset = buffer.getLong  // Offset 80
    val dataLength = buffer.getLong  // Offset 88
    
    val minInt = buffer.getInt       // Offset 96 (Union min_int)
    val maxInt = buffer.getInt       // Offset 100 (Union max_int)

    colId shouldBe 0
    minInt shouldBe 0    // Zone Map check
    maxInt shouldBe 99   // Zone Map check

    // --- C. CHECK ALIGNMENT ---
    // [BlockHeader 64] + [ColHeader 64] = 128 bytes total metadata.
    // Alignment Padding to 256 = 128 bytes.
    // Data Offset MUST be 256.
    
    val expectedDataOffset = 256
    dataOffset shouldBe expectedDataOffset

    // --- D. CHECK DATA ---
    buffer.position(expectedDataOffset)
    val val0 = buffer.getInt
    val val1 = buffer.getInt
    
    val0 shouldBe 0
    val1 shouldBe 1
    
    channel.close()
  }

  it should "load a Block back from disk and verify data integrity" in {
    // 1. Load the block using the C++ Loader
    val loadedPtr = NativeBridge.loadBlockFromFile(BLOCK_FILE)
    loadedPtr should not be 0
    
    // 2. Check Metadata
    val rows = NativeBridge.getRowCount(loadedPtr)
    rows shouldBe 100
    
    // 3. Verify Data Round-Trip
    val colPtr = NativeBridge.getColumnPtr(loadedPtr, 0)
    val outArray = new Array[Int](100)
    NativeBridge.copyToScala(colPtr, outArray, 100)
    
    outArray(0) shouldBe 0
    outArray(99) shouldBe 99
    
    NativeBridge.freeMainStore(loadedPtr)
    cleanUp()
  }
}