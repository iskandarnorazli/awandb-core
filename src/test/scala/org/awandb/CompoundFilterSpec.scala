/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 */

package org.awandb.core.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import java.io.File

class CompoundFilterSpec extends AnyFunSuite with BeforeAndAfterAll {

  val dataDir = "data_compound_test"
  var table: AwanTable = _

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }

  override def beforeAll(): Unit = {
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
    dir.mkdirs()

    table = new AwanTable("users", 1000, dataDir)
    table.addColumn("id")      
    table.addColumn("age")   
    table.addColumn("score")   
    
    SQLHandler.register("users", table)
    
    // Insert test data
    table.insertRow(Array(1, 20, 100))
    table.insertRow(Array(2, 30, 500))
    table.insertRow(Array(3, 25, 200))
    table.insertRow(Array(4, 40, 50)) 
    table.insertRow(Array(5, 60, 900))
    
    table.flush()
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    val dir = new File(dataDir)
    if (dir.exists()) deleteRecursively(dir)
  }

  // -------------------------------------------------------------------------
  // COMPOUND FILTER (AND / OR) TESTS
  // -------------------------------------------------------------------------

  test("1. Compound: Should handle AND logic correctly") {
    // Looking for someone between 20 and 40 with a score > 100
    // User 1: Score too low
    // User 2: Perfect (Age 30, Score 500)
    // User 3: Perfect (Age 25, Score 200)
    // User 4: Score too low
    // User 5: Age too high
    val sql = "SELECT id, age, score FROM users WHERE age < 40 AND score > 100"
    val res = SQLHandler.execute(sql)
    
    assert(res.contains("2 | 30 | 500"), "Missed User 2")
    assert(res.contains("3 | 25 | 200"), "Missed User 3")
    assert(!res.contains("1 | 20 | 100"), "Included User 1 incorrectly")
    assert(!res.contains("4 | 40 | 50"), "Included User 4 incorrectly")
    assert(!res.contains("5 | 60 | 900"), "Included User 5 incorrectly")
  }

  test("2. Compound: Should handle OR logic correctly") {
    // Looking for young people OR high scorers
    val sql = "SELECT id FROM users WHERE age < 22 OR score > 800"
    val res = SQLHandler.execute(sql)
    
    assert(res.contains("   1"), "Missed young user (User 1)")
    assert(res.contains("   5"), "Missed high scorer (User 5)")
    assert(!res.contains("   2"), "Included user not meeting either condition")
    assert(!res.contains("   3"), "Included user not meeting either condition")
  }
}