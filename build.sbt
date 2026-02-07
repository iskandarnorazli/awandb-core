name := "AwanDB"
version := "0.1"
scalaVersion := "3.3.5"

// Enable forking (Required for JVM options to work)
fork := true

// Fix: Define the path INSIDE the setting assignment using .value
run / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"
Test / javaOptions += s"-Djava.library.path=${baseDirectory.value}/lib/Release"

// Allow typing in the terminal for the interactive app
connectInput in run := true 

// Dependencies
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test

// ... inside your project settings ...
parallelExecution in Test := false