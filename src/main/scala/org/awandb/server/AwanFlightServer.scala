package org.awandb.server

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.awandb.core.engine.AwanTable
import java.io.File

object AwanFlightServer {
  
  def main(args: Array[String]): Unit = {
    // [CRITICAL FIX] Force Arrow to use Netty Allocator
    System.setProperty("arrow.memory.allocator", "org.apache.arrow.memory.NettyAllocationManager")
    // [CRITICAL] Disable Arrow's debug allocator (saves performance + avoids some native bugs)
    System.setProperty("arrow.memory.debug.allocator", "false")
    
    println("\n==========================================")
    println("   ☁️  AwanDB Flight Server (Native)     ")
    println("==========================================")
    
    // 1. Initialize Engine (Embedded)
    val dataDir = if (args.length > 0) args(0) else "data/flight_db"
    new File(dataDir).mkdirs()
    
    println(s"[Init] Loading database from: $dataDir")
    val table = new AwanTable("flight_demo", 1_000_000, dataDir)
    
    // 2. Start Networking (Arrow Flight)
    val allocator = new RootAllocator()
    val producer = new AwanFlightProducer(table, allocator)
    val location = Location.forGrpcInsecure("0.0.0.0", 3000)
    
    val server = FlightServer.builder(allocator, location, producer).build()
    server.start()
    
    println(s"[Ready] Listening on ${location.getUri}")
    println(s"[Info]  Mode: Standalone Executable")
    
    // Keep alive
    server.awaitTermination()
  }
}