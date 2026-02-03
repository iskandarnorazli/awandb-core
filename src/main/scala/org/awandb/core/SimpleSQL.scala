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
import scala.util.matching.Regex

class SimpleSQL(tables: Map[String, AwanTable]) {
  
  // Regex Patterns for Basic SQL
  val SelectPattern = """(?i)SELECT\s+(\w+)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*>\s*(\d+)""".r
  val InsertPattern = """(?i)INSERT\s+INTO\s+(\w+)\s+VALUES\s+\((.+)\)""".r
  val SavePattern   = """(?i)SAVE\s+SNAPSHOT""".r
  val LoadPattern   = """(?i)LOAD\s+SNAPSHOT""".r

  def execute(query: String): Unit = {
    query.trim match {
      case SelectPattern(targetCol, tableName, filterCol, thresholdStr) =>
        if (tables.contains(tableName)) {
           val start = System.nanoTime()
           
           // ADAPTER: Calls the new Engine API
           // The new query() uses AVX-512 to count matches incredibly fast.
           // For this MVP, it returns the count of rows matching the criteria.
           val count = tables(tableName).query(thresholdStr.toInt)
           
           val duration = (System.nanoTime() - start) / 1e6
           printResults(count, duration)
        } else println(s"   ERROR: Table '$tableName' not found.")

      case InsertPattern(tableName, valuesStr) =>
        if (tables.contains(tableName)) {
          try {
            // Parse "10, 20, 30" -> Array(10, 20, 30)
            val values = valuesStr.split(",").map(_.trim.toInt)
            tables(tableName).insertRow(values)
            println("   Status: Inserted row (Delta Store)")
          } catch {
             case e: Exception => println(s"   ERROR: ${e.getMessage}")
          }
        } else println(s"   ERROR: Table '$tableName' not found.")

      case SavePattern() =>
        // Map "SAVE SNAPSHOT" to the new flush() mechanism
        tables.values.foreach(_.flush())
        println("   Snapshot Saved (Flushed RAM to Disk Blocks).")

      case LoadPattern() =>
        // The BlockManager now handles loading automatically/lazily.
        println("   [INFO] Snapshot loading is handled automatically by BlockManager.")

      case _ => println("   ERROR: Invalid Syntax.")
    }
  }

  def printResults(count: Int, timeMs: Double): Unit = {
    println(f"   Status: OK (${timeMs}%.3f ms)")
    println(s"   Matched: $count rows")
  }
}