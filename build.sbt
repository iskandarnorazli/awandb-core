name := "AwanDB"
version := "0.1.0-alpha" // Updated to match your release tag
scalaVersion := "3.3.5"

// --- Run Configuration ---
fork := true
run / connectInput := true 

// [CRITICAL] Default main class (Flight Server)
Compile / mainClass := Some("org.awandb.server.AwanFlightServer")

// Native Library Path Support
// We look in 'lib/Release' for local dev and 'src/main/resources/native' for the embedded logic
run / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"
Test / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"

// Ensure tests run sequentially to avoid JNI memory race conditions
Test / parallelExecution := false

// --- Dependencies ---
val arrowVersion = "15.0.0"

libraryDependencies ++= Seq(
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  
  // Parallel Collections (Required for MorselExec)
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
  
  // SQL Parsing
  "com.github.jsqlparser" % "jsqlparser" % "4.7",

  // --- Apache Arrow Flight (High-Speed Networking) ---
  "org.apache.arrow" % "arrow-vector" % arrowVersion,
  "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
  "org.apache.arrow" % "flight-core" % arrowVersion,
  "org.apache.arrow" % "flight-grpc" % arrowVersion,
  
  // --- Logging (Essential for GraalVM/Netty debugging) ---
  "org.slf4j" % "slf4j-simple" % "2.0.9"
)

// --- Assembly (Fat JAR) Configuration ---
// This ensures that the DLL, SO, and DYLIB files built by GitHub are all included
assembly / assemblyMergeStrategy := {
  case PathList("native", xs @ _*) => MergeStrategy.first // Keep our native engines
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "services" :: xs => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case "module-info.class" => MergeStrategy.discard
  case x => MergeStrategy.first
}

// --- GraalVM Native Image Configuration ---
enablePlugins(NativeImagePlugin)

nativeImageVersion := "21.0.2"
nativeImageJvm := "graalvm-java21" 

nativeImageOptions ++= Seq(
  "--no-fallback",
  "--allow-incomplete-classpath",
  "--enable-url-protocols=http,https",
  "-H:+JNI",                      
  "-H:+ReportExceptionStackTraces",
  // Essential for JSqlParser and Arrow to work in a binary
  "--initialize-at-build-time=org.slf4j",
  "--initialize-at-run-time=io.netty",   
  "--initialize-at-run-time=org.apache.arrow.memory.NettyAllocationManager",
  // Allow the engine to see its own native methods
  "-H:IncludeResources=native/.*" 
)