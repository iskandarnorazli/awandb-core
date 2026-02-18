package org.awandb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.flight.grpc.CredentialCallOption
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import org.awandb.core.jni.NativeBridge

import org.awandb.server.AwanFlightSqlProducer
import org.awandb.server.auth.AwanAuthHandler
import scala.jdk.CollectionConverters._
import java.io.File

class AwanFlightSqlTest extends AnyFunSuite with BeforeAndAfterAll {

  private var allocator: RootAllocator = _
  private var server: FlightServer = _
  private var testTable: AwanTable = _
  private val port = 45678 
  private val testDataDir = new File("./data/test")

  // Helper to completely erase the test database files
  private def cleanDatabaseDirectory(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) file.listFiles().foreach(cleanDatabaseDirectory)
      file.delete()
    }
  }

  override def beforeAll(): Unit = {
    // 1. Guarantee a pristine environment before starting
    cleanDatabaseDirectory(testDataDir)
    testDataDir.mkdirs()

    // 2. Initialize Native Engine & Schema
    NativeBridge.init()
    testTable = new AwanTable("users", 1000, testDataDir.getAbsolutePath)
    testTable.addColumn("id")
    testTable.addColumn("username", isString = true)
    SQLHandler.register("users", testTable)

    // 3. Start the Secure Arrow Flight SQL Server
    allocator = new RootAllocator()
    val location = Location.forGrpcInsecure("127.0.0.1", port)
    
    // [FIX] Inject the testTable into the producer constructor
    val producer = new AwanFlightSqlProducer(allocator, location, testTable)
    val authHandler = new AwanAuthHandler()

    server = FlightServer.builder(allocator, location, producer)
      .headerAuthenticator(authHandler)
      .build()
    
    server.start()
  }

  override def afterAll(): Unit = {
    // 1. Gracefully shutdown the server and free C++ memory
    if (server != null) server.close()
    if (testTable != null) testTable.close()
    if (allocator != null) allocator.close()

    // 2. Nuke the database files so the next test run is clean
    cleanDatabaseDirectory(testDataDir)
  }

  // ==========================================
  // TEST CASES
  // ==========================================

  // --- Auth & Error Handling ---

  test("1. Authentication should reject invalid credentials") {
    val clientAllocator = new RootAllocator()
    val client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", port)).build()
    val sqlClient = new FlightSqlClient(client)
    val badAuth = new CredentialCallOption(new BasicAuthCredentialWriter("admin", "wrong_password"))
    
    val exception = intercept[FlightRuntimeException] {
      sqlClient.getTables(null, null, null, null, false, badAuth)
    }
    assert(exception.status().code() == FlightStatusCode.UNAUTHENTICATED)
    
    client.close(); clientAllocator.close()
  }

  test("2. Error Handling: Should gracefully reject invalid SQL syntax") {
    val clientAllocator = new RootAllocator()
    val client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", port)).build()
    val sqlClient = new FlightSqlClient(client)
    val validAuth = new CredentialCallOption(new BasicAuthCredentialWriter("admin", "admin"))

    val exception = intercept[FlightRuntimeException] {
      val info = sqlClient.execute("SELECT * FROM non_existent_table", validAuth)
      client.getStream(info.getEndpoints.get(0).getTicket, validAuth).next()
    }
    
    // We expect the server to catch the missing table and return a proper gRPC error, NOT hang or crash
    assert(exception.getMessage.contains("Error: Table"))
    
    client.close(); clientAllocator.close()
  }

  // --- CRUD Pipeline ---

  test("3. Write Pipeline: Should execute INSERT over Flight SQL") {
    val clientAllocator = new RootAllocator()
    val client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", port)).build()
    val sqlClient = new FlightSqlClient(client)
    val validAuth = new CredentialCallOption(new BasicAuthCredentialWriter("admin", "admin"))

    val affectedRows = sqlClient.executeUpdate("INSERT INTO users VALUES (1, 'TestUser')", validAuth)
    assert(affectedRows == 1L)

    client.close(); clientAllocator.close()
  }

  test("4. Read Pipeline: Should execute SELECT and verify the inserted row") {
    val clientAllocator = new RootAllocator()
    val client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", port)).build()
    val sqlClient = new FlightSqlClient(client)
    val validAuth = new CredentialCallOption(new BasicAuthCredentialWriter("admin", "admin"))

    val flightInfo = sqlClient.execute("SELECT * FROM users", validAuth)
    val stream = client.getStream(flightInfo.getEndpoints.get(0).getTicket, validAuth)
    val root = stream.getRoot

    var foundData = ""
    while (stream.next()) {
      val resultVector = root.getVector("query_result").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      foundData += new String(resultVector.get(0), "UTF-8")
    }

    assert(foundData.contains("TestUser"))

    stream.close(); root.close(); client.close(); clientAllocator.close()
  }

  test("5. Update Pipeline: Should UPDATE the row and verify changes") {
    val clientAllocator = new RootAllocator()
    val client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", port)).build()
    val sqlClient = new FlightSqlClient(client)
    val validAuth = new CredentialCallOption(new BasicAuthCredentialWriter("admin", "admin"))

    // Execute Update
    sqlClient.executeUpdate("UPDATE users SET username = 'UpdatedUser' WHERE id = 1", validAuth)

    // Verify Update
    val flightInfo = sqlClient.execute("SELECT * FROM users", validAuth)
    val stream = client.getStream(flightInfo.getEndpoints.get(0).getTicket, validAuth)
    val root = stream.getRoot
    var foundData = ""
    while (stream.next()) {
      val resultVector = root.getVector("query_result").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      foundData += new String(resultVector.get(0), "UTF-8")
    }

    assert(foundData.contains("UpdatedUser"))
    assert(!foundData.contains("TestUser")) // Old data should be gone

    stream.close(); root.close(); client.close(); clientAllocator.close()
  }

  test("6. Delete Pipeline: Should DELETE the row") {
    val clientAllocator = new RootAllocator()
    val client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", port)).build()
    val sqlClient = new FlightSqlClient(client)
    val validAuth = new CredentialCallOption(new BasicAuthCredentialWriter("admin", "admin"))

    // Execute Delete
    sqlClient.executeUpdate("DELETE FROM users WHERE id = 1", validAuth)

    // Verify Delete
    val flightInfo = sqlClient.execute("SELECT * FROM users", validAuth)
    val stream = client.getStream(flightInfo.getEndpoints.get(0).getTicket, validAuth)
    val root = stream.getRoot
    var foundData = ""
    while (stream.next()) {
      val resultVector = root.getVector("query_result").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      foundData += new String(resultVector.get(0), "UTF-8")
    }

    // Since our parser formats the output as "Found Rows: \n", we just check it doesn't contain the user
    assert(!foundData.contains("UpdatedUser"))

    stream.close(); root.close(); client.close(); clientAllocator.close()
  }

  // --- Native Network JSON Ingestion ---

  test("7. Custom RPC: Should ingest JSON payload via doAction bypassing SQL") {
    val clientAllocator = new RootAllocator()
    val client = FlightClient.builder(clientAllocator, Location.forGrpcInsecure("127.0.0.1", port)).build()
    val validAuth = new CredentialCallOption(new BasicAuthCredentialWriter("admin", "admin"))

    // 1. Send the JSON payload via the custom Action endpoint
    val jsonPayload = """[{"id": 99, "username": "JsonUser"}]"""
    val action = new Action("IngestJson:users", jsonPayload.getBytes("UTF-8"))
    
    val results = client.doAction(action, validAuth)
    var responseStr = ""
    while (results.hasNext) {
      responseStr += new String(results.next().getBody, "UTF-8")
    }
    
    // Ensure the server returned the correct insertion count
    assert(responseStr.contains("\"inserted\": 1"))

    // 2. Read the data back using standard SQL to verify it landed in the columnar table
    val sqlClient = new FlightSqlClient(client)
    val flightInfo = sqlClient.execute("SELECT * FROM users", validAuth)
    val stream = client.getStream(flightInfo.getEndpoints.get(0).getTicket, validAuth)
    val root = stream.getRoot
    var foundData = ""
    while (stream.next()) {
      val resultVector = root.getVector("query_result").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      foundData += new String(resultVector.get(0), "UTF-8")
    }

    assert(foundData.contains("JsonUser"))

    stream.close(); root.close(); client.close(); clientAllocator.close()
  }
}