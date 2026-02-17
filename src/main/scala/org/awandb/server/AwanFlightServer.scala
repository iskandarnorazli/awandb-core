/*
 * Copyright 2026 Mohammad Iskandar Sham Bin Norazli Sham
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.awandb.server

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight.{FlightServer, Location}
import org.awandb.core.engine.AwanTable
import org.awandb.core.sql.SQLHandler
import org.awandb.core.jni.NativeBridge
import org.awandb.server.AwanFlightProducer

object AwanFlightServer {

  def main(args: Array[String]): Unit = {
    println("==========================================")
    println("   ☁️  AwanDB Flight Server (Native)     ")
    println("==========================================")

    // Default Configuration
    var port = 3000
    var dataDir = "./data/default"

    // Smart CLI Argument Parsing
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--port" if i + 1 < args.length => 
          port = args(i + 1).toInt
          i += 2
        case "--data-dir" if i + 1 < args.length => 
          dataDir = args(i + 1)
          i += 2
        case _ => 
          i += 1
      }
    }

    println(s"[Init] Binding to Port: $port")
    println(s"[Init] Loading database from: $dataDir")

    // Initialize Native Engine
    NativeBridge.init()

    // Setup a default table for testing
    val defaultTable = new AwanTable("leaderboard", 100000, dataDir)
    defaultTable.addColumn("id")
    defaultTable.addColumn("player_name", isString = true)
    defaultTable.addColumn("score")
    SQLHandler.register("leaderboard", defaultTable)

    val allocator = new RootAllocator()
    val location = Location.forGrpcInsecure("0.0.0.0", port)
    
    // 1. Initialize the new Flight SQL Producer
    val producer = new AwanFlightSqlProducer(allocator, location)

    // 2. Initialize the Auth Handler
    val authHandler = new org.awandb.server.auth.AwanAuthHandler()

    // 3. Build the server with Auth Enabled
    val server = FlightServer.builder(allocator, location, producer)
      .headerAuthenticator(authHandler) // [FIX] Pass the handler directly!
      .build()

    try {
      server.start()
      println(s"🚀 Server is fully operational and listening on grpc://0.0.0.0:$port")
      server.awaitTermination()
    } catch {
      case e: Exception =>
        System.err.println(s"❌ Fatal Server Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      defaultTable.close()
      allocator.close()
    }
  }
}