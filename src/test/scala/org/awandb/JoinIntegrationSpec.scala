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

class JoinIntegrationSpec extends AnyFlatSpec with Matchers {

  "HashJoin" should "NOT join deleted rows" in {
    // 1. Setup Probe Table (Orders)
    val orders = new AwanTable("orders", 1000, "data_test_join")
    orders.addColumn("id")      // PK
    orders.addColumn("user_id") // FK
    orders.addColumn("amount")

    // 2. Setup Build Table (Users)
    val users = new AwanTable("users", 1000, "data_test_join")
    users.addColumn("id")       // PK
    users.addColumn("region")

    // 3. Insert Data
    // User 1 -> Region 10
    // User 2 -> Region 20
    users.insertRow(Array(1, 10))
    users.insertRow(Array(2, 20))

    // Order 100 -> User 1 (Amount 500)
    // Order 101 -> User 2 (Amount 600)
    orders.insertRow(Array(100, 1, 500))
    orders.insertRow(Array(101, 2, 600))
    
    // Flush to force disk scan path (where TableScanOperator lives)
    users.flush()
    orders.flush()

    // 4. DELETE User 1
    // This means Order 100 should now have NO MATCH in a Join
    users.delete(1) should be (true)

    // 5. Execute Join: SELECT sum(amount) JOIN users on user_id = id
    // We expect only Order 101 (User 2) to match. Sum should be 600.
    
    // Note: You need to expose a method in AwanTable to run a join for this test
    // Assuming executeJoin(otherTable, joinColLeft, joinColRight, aggCol)
    
    // For TDD: If you haven't implemented executeJoin yet, verify manually via query
    // But ideally, we run the actual Operator DAG here.
    
    // Let's verify the components:
    // Does TableScan on 'users' return ID 1? It SHOULD NOT.
    val countUser1 = users.query("id", 1)
    countUser1 should be (0) // If this fails, your TableScanOperator ignores delete bitmaps!
    
    users.close()
    orders.close()
  }
}