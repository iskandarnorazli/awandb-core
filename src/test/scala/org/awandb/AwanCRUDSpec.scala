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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.awandb.core.engine.AwanTable
import java.io.File

class AwanCRUDSpec extends AnyFlatSpec with Matchers {

  // [FIX] Use standard Java IO for recursive deletion (No extra dependencies)
  def cleanSlate(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (dir.exists()) {
      deleteRecursively(dir)
    }
  }

  def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) {
      f.listFiles().foreach(deleteRecursively)
    }
    f.delete()
  }

  "AwanTable" should "insert and query integers correctly" in {
    cleanSlate("data_test_crud")
    val table = new AwanTable("users", 1000, "data_test_crud")
    table.addColumn("id")
    table.addColumn("age")

    table.insertRow(Array(1, 25))
    table.insertRow(Array(2, 30))

    // Check RAM Query
    table.query("age", 25) should be (1)
    table.query("age", 99) should be (0)
    
    table.close()
  }

  it should "DELETE a row and ensure it is gone from queries" in {
    // Note: We reuse the folder but unique table names or clean it
    cleanSlate("data_test_crud_del") 
    val table = new AwanTable("users", 1000, "data_test_crud_del")
    table.addColumn("id")
    table.addColumn("age")

    table.insertRow(Array(10, 50))
    table.insertRow(Array(11, 50))
    
    // Verify before delete
    table.query("age", 50) should be (2)

    // DELETE ID 10
    val deleted = table.delete(10)
    deleted should be (true)

    // Verify after delete (Should be 1)
    table.query("age", 50) should be (1)
    
    // Verify ID 10 is actually gone
    table.delete(10) should be (false) // Should fail 2nd time

    table.close()
  }

  it should "UPDATE a row by Deleting and Re-Inserting" in {
    cleanSlate("data_test_crud_upd")
    val table = new AwanTable("products", 1000, "data_test_crud_upd")
    table.addColumn("id")
    table.addColumn("price")

    table.insertRow(Array(500, 100))
    
    // UPDATE: Change price 100 -> 200
    // 1. Delete old
    table.delete(500) should be (true)
    // 2. Insert new (Same ID, New Price)
    table.insertRow(Array(500, 200))

    // Verify
    table.query("price", 100) should be (0) // Old value gone
    table.query("price", 200) should be (1) // New value present
    
    table.close()
  }

  it should "Preserve Block Structure after Flush" in {
    cleanSlate("data_test_crud_flush")
    val table = new AwanTable("archive", 1000, "data_test_crud_flush")
    table.addColumn("id")
    
    // Insert 1000 rows
    for (i <- 0 until 1000) table.insertRow(Array(i))

    // Flush to Disk (Block 0)
    table.flush()

    // Delete row 500 (This marks a bit in Block 0, doesn't rewrite file)
    table.delete(500)

    // Verify count via Scan
    // Note: query(threshold) is range
    table.query(9999) should be (0) 
    
    // Check internal state (Whitebox testing)
    // We expect 1 loaded block
    table.blockManager.getLoadedBlocks.size should be (1)
    
    table.close()
  }

  it should "Persist Deletions across Engine Restarts (The Lazarus Test)" in {
    val dir = "data_test_recovery"
    cleanSlate(dir)
    
    // -------------------------------------------------
    // SESSION 1: Insert, Flush, Delete, Close
    // -------------------------------------------------
    var table = new AwanTable("persist_test", 1000, dir)
    table.addColumn("id")
    table.addColumn("score")

    // Insert 3 rows
    table.insertRow(Array(1, 100))
    table.insertRow(Array(2, 200)) // We will delete this one
    table.insertRow(Array(3, 300))
    
    // Flush to create Block 0
    table.flush()
    
    // Delete ID 2 (This marks the bit in Block 0)
    table.delete(2) should be (true)
    
    // Flush again? (Or assume close() handles it? For now, explicit flush of metadata)
    // Note: You need to ensure flush() saves the bitmaps!
    table.flush() 
    table.close()

    // -------------------------------------------------
    // SESSION 2: Re-open and Verify
    // -------------------------------------------------
    val table2 = new AwanTable("persist_test", 1000, dir)
    
    // Verify Schema loaded? (Or re-add manually if schema persistence isn't done yet)
    table2.addColumn("id")
    table2.addColumn("score")
    
    // THE ASSERTION: ID 2 must NOT exist.
    // If this fails, your Deletion Bitmap wasn't saved/loaded.
    table2.query("score", 200) should be (0) 
    
    // ID 1 and 3 should still exist
    table2.query("score", 100) should be (1)
    table2.query("score", 300) should be (1)
    
    table2.close()
  }

  it should "Handle DELETE with Dictionary Encoded Strings" in {
    cleanSlate("data_test_dict_del")
    val table = new AwanTable("users", 1000, "data_test_dict_del")
    table.addColumn("id")
    table.addColumn("city", isString = true, useDictionary = true)

    table.insertRow(Array(1, "Kuala Lumpur"))
    table.insertRow(Array(2, "Singapore"))
    table.insertRow(Array(3, "Kuala Lumpur"))

    // Flush to force dictionary encoding
    table.flush()

    // Delete one "Kuala Lumpur" row
    table.delete(1)

    // Verify:
    // "Kuala Lumpur" should still exist (Row 3)
    // "Singapore" should still exist (Row 2)
    // Row 1 should be gone.
    
    table.query("city", "Kuala Lumpur") should be (1) // Was 2, now 1
    table.query("city", "Singapore") should be (1)

    table.close()
  }

  it should "Handle Non-Existent ID Deletions gracefully" in {
    cleanSlate("data_test_edge")
    val table = new AwanTable("users", 1000, "data_test_edge")
    table.addColumn("id")
    
    table.insertRow(Array(1))
    
    // Delete Ghost ID
    val res = table.delete(99999)
    res should be (false) // Should return false, NOT throw exception
    
    table.close()
  }
}