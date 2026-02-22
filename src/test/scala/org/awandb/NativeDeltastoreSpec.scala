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

import org.scalatest.funsuite.AnyFunSuite
import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge
import org.awandb.core.sql.SQLHandler
import java.io.File

class NativeDeltastoreSpec extends AnyFunSuite {

  NativeBridge.init()

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  test("Native Deltastore: Should insert natively and delete lock-free via bitmask") {
    
    val uniqueTestDir = s"./data/test_deltastore_${System.currentTimeMillis()}"
    val testDirFile = new File(uniqueTestDir)
    testDirFile.mkdirs()

    val table = new AwanTable("players", 1000, uniqueTestDir)
    table.addColumn("id")    // Col 0
    table.addColumn("score") // Col 1
    
    SQLHandler.register("players", table)

    // ==========================================
    // 1. FAST NATIVE APPENDS 
    // Bypassing JVM Arrays, writing straight to C++
    // ==========================================
    table.insertRow(Array(1, 100))
    table.insertRow(Array(2, 200))
    table.insertRow(Array(3, 300))
    table.insertRow(Array(4, 400))
    
    // ==========================================
    // 2. VERIFY INITIAL STATE
    // ==========================================
    val sumBefore = SQLHandler.execute("SELECT id, SUM(score) FROM players GROUP BY id")
    assert(sumBefore.contains("1 | 100"))
    assert(sumBefore.contains("2 | 200"))
    assert(sumBefore.contains("3 | 300"))
    assert(sumBefore.contains("4 | 400"))

    // ==========================================
    // 3. LOCK-FREE NATIVE DELETION
    // ==========================================
    val deleteSuccess = table.delete(3) // Delete ID 3
    assert(deleteSuccess, "Delete operation should return true for existing row")

    val deleteFail = table.delete(99) // Delete non-existent ID
    assert(!deleteFail, "Delete operation should return false for missing row")

    // ==========================================
    // 4. VERIFY DELETED STATE
    // ==========================================
    val sumAfter = SQLHandler.execute("SELECT id, SUM(score) FROM players GROUP BY id")
    
    assert(sumAfter.contains("1 | 100"))
    assert(sumAfter.contains("2 | 200"))
    assert(!sumAfter.contains("3 |"), "ID 3 should be masked out by the native bitmask!")
    assert(sumAfter.contains("4 | 400"))

    // Cleanup
    table.close()
    deleteRecursively(testDirFile)
  }
}