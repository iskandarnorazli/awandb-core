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
 * AwanDB - High Performance HTAP Engine
 */

package org.awandb.core

import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import org.awandb.core.jni.NativeBridge
import scala.io.StdIn

object MainApp extends App {
  println(">> Booting AwanDB Server (Phase 6: SQL Mode)...")

  // 1. Initialize Native Backend (Hardware Discovery)
  NativeBridge.init()

  // 2. Setup The "Products" Table
  // We use 1M row capacity for this test.
  val productsTable = new AwanTable("products", 1_000_000)
  
  // Define Schema (Column 0 must be ID for now)
  productsTable.addColumn("id")    // Primary Key
  productsTable.addColumn("price") // Integer
  productsTable.addColumn("stock") // Integer
  
  // Register with the SQL Engine
  SQLHandler.register("products", productsTable)

  println(">> System Ready.")
  println(">> Pre-seeding data...")
  
  // Seed a few rows to test deletions immediately
  productsTable.insertRow(Array(100, 50, 10))
  productsTable.insertRow(Array(101, 99, 20))
  productsTable.insertRow(Array(102, 150, 5))
  println(">> Seeded 3 rows: IDs [100, 101, 102]")

  // 3. Interactive SQL Console
  println("\n" + "="*50)
  println(" AwanDB SQL Console v2026.03")
  println("="*50)
  println(" Examples:")
  println("   INSERT INTO products VALUES (103, 45, 100)")
  println("   DELETE FROM products WHERE id = 100")
  println("   UPDATE products SET price = 0 WHERE id = 101")
  println("   flush  (Force data to disk)")
  println("   exit   (Shutdown)")
  println("-" * 50)

  var running = true
  while (running) {
    print("\nsql> ") 
    val input = StdIn.readLine()

    if (input == null || input == "exit") {
      running = false
    } else if (input.trim.nonEmpty) {
      val cmd = input.trim
      
      // Special System Commands
      if (cmd.equalsIgnoreCase("flush")) {
        val start = System.nanoTime()
        productsTable.flush()
        val dur = (System.nanoTime() - start) / 1e6
        println(f">> System Flushed to Disk (${dur}%.2f ms).")
      } 
      // Standard SQL Commands
      else {
        val result = SQLHandler.execute(cmd)
        println(s">> ${result.message}")
      }
    }
  }

  println(">> Shutting down...")
  productsTable.close()
}