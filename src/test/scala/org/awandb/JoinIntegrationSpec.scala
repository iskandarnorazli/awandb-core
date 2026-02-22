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

class JoinIntegrationSpec extends AnyFlatSpec with Matchers {

  def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        val children = file.listFiles()
        if (children != null) children.foreach(deleteRecursively)
      }
      file.delete()
    }
  }

  "HashJoin" should "NOT join deleted rows" in {
    val testDir = "data_test_join"
    deleteRecursively(new File(testDir))
    new File(testDir).mkdirs()

    val orders = new AwanTable("orders", 1000, testDir)
    orders.addColumn("id")      
    orders.addColumn("user_id") 
    orders.addColumn("amount")

    val users = new AwanTable("users", 1000, testDir)
    users.addColumn("id")       
    users.addColumn("region")

    users.insertRow(Array(1, 10))
    users.insertRow(Array(2, 20))

    orders.insertRow(Array(100, 1, 500))
    orders.insertRow(Array(101, 2, 600))
    
    users.flush()
    orders.flush()

    users.delete(1) should be (true)

    val countUser1 = users.query("id", 1)
    countUser1 should be (0) 
    
    users.close()
    orders.close()
  }
}