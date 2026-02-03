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

package org.awandb.core

import org.awandb.core.engine.AwanTable
import org.awandb.core.jni.NativeBridge
import scala.util.Random
import scala.io.StdIn

object MainApp extends App {
  println(">> Booting AwanDB Server...")

  // ==================================================================================
  // [OPEN CORE ARCHITECTURE] PLUGIN DISCOVERY
  // ==================================================================================
  val isEnterprise = try {
    // Check for Enterprise existence (Simulated)
    false 
  } catch {
    case _: ClassNotFoundException => false
  }

  // 1. INITIALIZE NATIVE BRIDGE (Hardware Discovery)
  NativeBridge.init()

  if (isEnterprise) {
    println(">> [LICENSE] ENTERPRISE EDITION ACTIVE")
    println(">> [SYSTEM] OverFlow Guard: ENABLED")
    println(">> [SYSTEM] Usage Metering: ENABLED")
  } else {
    println(">> [LICENSE] OPEN SOURCE CORE (Community Edition)")
  }

  // ==================================================================================
  // DATA SETUP
  // ==================================================================================
  val rows = 1_000_000
  // FIX: Use AwanTable instead of DatabaseTable
  val productsTable = new AwanTable("products", rows)
  
  productsTable.addColumn("id")
  productsTable.addColumn("price")
  productsTable.addColumn("stock")

  println(s">> Seeding $rows rows (Columnar Layout)...")
  
  // Generate data arrays
  val ids = (0 until rows).toArray
  val prices = Array.fill(rows)(Random.nextInt(100))
  val stocks = Array.fill(rows)(Random.nextInt(500))

  // FIX: Load data using the new insertRow API
  // In a real bulk-load scenario, we would use a specialized loader, 
  // but for this prototype, we iterate.
  var i = 0
  while (i < rows) {
    productsTable.insertRow(Array(ids(i), prices(i), stocks(i)))
    i += 1
  }
  println(">> Seeding Complete.")

  // 2. Initialize SQL Engine
  val catalog = Map("products" -> productsTable)
  val sqlEngine = new SimpleSQL(catalog)

  // 3. Interactive Console
  println("\n" + "="*40)
  println(" AwanDB SQL Console")
  println(" Try: SELECT stock FROM products WHERE price > 90")
  println(" Type 'exit' to quit.")
  println("="*40)

  var running = true
  while (running) {
    print("\nsql> ") // Prompt
    
    // Read input safely
    val input = StdIn.readLine()

    if (input == null) {
      // FIX: If keyboard is disconnected (Ctrl+C or EOF), stop immediately
      running = false 
    } else if (input == "exit") {
      running = false
    } else if (input.trim.nonEmpty) {
      try {
        sqlEngine.execute(input)
      } catch {
        case e: Exception => println(s"   ERROR: ${e.getMessage}")
      }
    }
  }

  println(">> Shutting down...")
  productsTable.close()
}