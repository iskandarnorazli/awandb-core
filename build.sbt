name := "AwanDB"
version := "0.1"
scalaVersion := "3.3.5"

// --- Run Configuration ---
fork := true
run / connectInput := true 

// [CRITICAL] Tell GraalVM where the 'main' function lives
Compile / mainClass := Some("org.awandb.server.AwanFlightServer")

// Define the path INSIDE the setting assignment using .value
run / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"
Test / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"

// Ensure tests run sequentially to avoid "Zombie Data" race conditions
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
assembly / assemblyMergeStrategy := {
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

// [CRITICAL FIX] Use GraalVM Community Edition 21 (Java 21)
// This forces SBT to ignore your old local installation and download a compatible one.
nativeImageVersion := "21.0.2"
nativeImageJvm := "graalvm-java21" 

nativeImageOptions ++= Seq(
  "--no-fallback",
  "--allow-incomplete-classpath",
  "--enable-url-protocols=http,https",
  "-H:+JNI",                      // Allow JNI access (for C++ Engine)
  "-H:+ReportExceptionStackTraces",
  "--initialize-at-build-time=org.slf4j",
  "--initialize-at-run-time=io.netty",   // Netty must initialize at runtime
  "--initialize-at-run-time=org.apache.arrow.memory.NettyAllocationManager"
)