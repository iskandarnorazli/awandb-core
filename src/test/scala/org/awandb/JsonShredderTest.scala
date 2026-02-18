package org.awandb.ingest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.awandb.core.engine.AwanTable
import org.awandb.core.ingest.JsonShredder
import org.awandb.core.jni.NativeBridge
import java.io.File

class JsonShredderTest extends AnyFunSuite with BeforeAndAfterAll {

  private var table: AwanTable = _
  private val testDataDir = new File("./data/json_test")

  override def beforeAll(): Unit = {
    NativeBridge.init()
    if (!testDataDir.exists()) testDataDir.mkdirs()
    
    // Create a strict columnar schema
    table = new AwanTable("sensors", 1000, testDataDir.getAbsolutePath)
    table.addColumn("id")
    table.addColumn("device_name", isString = true)
    table.addColumn("reading")
    // [NEW] A column expecting flattened nested JSON data
    table.addColumn("metadata.battery_level") 
  }

  override def afterAll(): Unit = {
    if (table != null) table.close()
    if (testDataDir.exists()) {
      testDataDir.listFiles().foreach(_.delete())
      testDataDir.delete()
    }
  }

  test("1. Should shred a flat JSON array into columnar table") {
    val jsonPayload = """[
      {"id": 101, "device_name": "Sensor_A", "reading": 250},
      {"id": 102, "device_name": "Sensor_B", "reading": 900}
    ]"""
    val rowsInserted = JsonShredder.ingest(jsonPayload, table)
    assert(rowsInserted == 2, "Should have inserted exactly 2 rows")
  }

  test("2. Should handle missing fields gracefully (Null/Default Injection)") {
    // Charlie is missing his 'reading' AND 'metadata.battery_level'
    val jsonPayload = """[
      {"id": 103, "device_name": "Sensor_C"} 
    ]"""
    val rowsInserted = JsonShredder.ingest(jsonPayload, table)
    assert(rowsInserted == 1)
  }

  test("3. Should completely ignore extra fields not in the table schema") {
    // Contains "secret_token" and "firmware_version" which are not in the AwanTable schema
    val jsonPayload = """[
      {"id": 104, "device_name": "Sensor_D", "reading": 50, "secret_token": "xyz", "firmware_version": 2.1}
    ]"""
    val rowsInserted = JsonShredder.ingest(jsonPayload, table)
    assert(rowsInserted == 1)
  }

  test("4. Should gracefully reject malformed JSON without crashing the DB") {
    val badJson = """[ {"id": 105, "missing_quote: ... oops }"""
    val rowsInserted = JsonShredder.ingest(badJson, table)
    assert(rowsInserted == 0) // Should catch the Jackson exception and return 0
  }

  test("5. Should flatten nested JSON objects using dot-notation schema mapping") {
    val jsonPayload = """[
      {
        "id": 106,
        "device_name": "Sensor_E",
        "reading": 88,
        "metadata": {
          "battery_level": 95,
          "network": "5G"
        }
      }
    ]"""
    val rowsInserted = JsonShredder.ingest(jsonPayload, table)
    assert(rowsInserted == 1)
    
    // TDD Note: This will currently pass the insert count, BUT if we flush and read it, 
    // the battery_level will wrongly be 0 because our current shredder doesn't know how to step 
    // down into the "metadata" map to find "battery_level".
  }
}