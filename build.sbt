name := "AwanDB"
version := "0.1.0-alpha"
scalaVersion := "3.3.5"

// --- Run Configuration ---
fork := true
run / connectInput := true 
// 2. Unlock the internal NIO modules that Arrow requires
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

// Default main class (Standalone Server)
Compile / mainClass := Some("org.awandb.server.AwanFlightServer")

// Local Development Native Library Path 
// (CI/CD passes this dynamically, but this allows local `sbt run` and `sbt test` to work)
run / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"
Test / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"

// ---------------------------------------------------------------------------
// JNI TEST ISOLATION CONFIGURATION
// Forces SBT to spawn a brand new, pristine JVM for every single test suite.
// This prevents C++ memory leaks, double-frees, and static state bleed.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// JNI TEST ISOLATION CONFIGURATION
// ---------------------------------------------------------------------------
Test / fork := true
Test / parallelExecution := false
Test / testGrouping := {
  val originalSettings = (Test / definedTests).value
  val baseDir = baseDirectory.value.getAbsolutePath

  originalSettings.map { test =>
    Tests.Group(
      name = test.name,
      tests = Seq(test),
      runPolicy = Tests.SubProcess(
        ForkOptions().withRunJVMOptions(Vector(
          "-Xmx4G", 
          s"-Djava.library.path=$baseDir/lib/Release",
          // [FIX] Arrow Native Memory Access Flags MUST be in this Vector!
          "--add-opens=java.base/java.nio=ALL-UNNAMED",
          "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
        ))
      )
    )
  }
}

// --- Dependencies ---
val arrowVersion = "15.0.0"

libraryDependencies ++= Seq(
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  
  // Parallel Collections (Required for Scala-side grouping/sorting)
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
  
  // SQL Parsing (ANSI SQL AST generation)
  "com.github.jsqlparser" % "jsqlparser" % "4.7",

  // --- Apache Arrow Flight (High-Speed Networking) ---
  "org.apache.arrow" % "arrow-vector" % arrowVersion,
  "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
  "org.apache.arrow" % "flight-core" % arrowVersion,
  "org.apache.arrow" % "flight-grpc" % arrowVersion,
  "org.apache.arrow" % "flight-sql" % arrowVersion,
  
  // --- Logging ---
  "org.slf4j" % "slf4j-simple" % "2.0.9"
)

// --- Assembly (Fat JAR) Configuration ---
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case x => MergeStrategy.first
}

// ---------------------------------------------------------------------------
// GRAALVM NATIVE IMAGE CONFIGURATION
// ---------------------------------------------------------------------------
enablePlugins(NativeImagePlugin)

// The class that contains your main() method
//Compile / nativeImageMainClass := "org.awandb.server.AwanFlightServer"
Compile / mainClass := Some("org.awandb.server.AwanServer")

// Name of the output executable
nativeImageOutput := file("target") / "native-image" / "awandb-server"

nativeImageOptions ++= Seq(
  "--no-fallback", // Force a pure native build (fail if it requires a JVM fallback)
  "-H:+ReportExceptionStackTraces",
  
  // 1. Enable JNI for your C++ Engine
  "-H:+JNI",
  
  // 2. Unlock Native Access for Apache Arrow / Netty
  "--enable-native-access=ALL-UNNAMED",
  "-J--add-opens=java.base/java.nio=ALL-UNNAMED",
  "-J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-J--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
  
  // 3. Initialize specific loggers at build time to prevent reflection crashes
  "--initialize-at-build-time=org.slf4j",
  "--initialize-at-build-time=scala.reflect"
)