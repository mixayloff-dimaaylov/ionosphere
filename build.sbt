import sbt._

organization := "com.cgnal"

name := "novatel-streaming"

version in ThisBuild := "1.0"

val assemblyName = "novatel-streaming-assembly"

scalaVersion in ThisBuild := "2.11.8"

scalariformSettings

scalastyleFailOnError := true

dependencyUpdatesExclusions := moduleFilter(organization = "org.scala-lang")

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
)

wartremoverErrors ++= Seq(
  Wart.Any2StringAdd,
  Wart.EitherProjectionPartial,
  Wart.Enumeration,
  Wart.Equals,
  Wart.ExplicitImplicitTypes,
  Wart.FinalVal,
  Wart.IsInstanceOf,
  Wart.JavaConversions,
  Wart.LeakingSealed,
  Wart.ListOps,
  Wart.MutableDataStructures,
  Wart.Null,
  Wart.Option2Iterable,
  Wart.OptionPartial,
  Wart.Throw,
  Wart.TryPartial,
  Wart.Var,
  Wart.While
)

val sparkVersion = "2.2.1"

val hadoopVersion = "2.6.0-cdh5.14.0"

val hbaseVersion = "1.2.0-cdh5.14.0"

val sparkAvroVersion = "3.1.0"

val scalaTestVersion = "3.0.1"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

resolvers += Resolver.typesafeRepo("releases")

val isALibrary = true //this is a library project

val sparkExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.apache.hadoop", "hadoop-client").
    exclude("org.apache.hadoop", "hadoop-yarn-client").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")

val assemblyDependencies = (scope: String) => Seq(
  "ch.hsr" % "geohash" % "1.3.0" % scope,
  sparkExcludes("org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % scope),
  sparkExcludes("com.databricks" %% "spark-avro" % sparkAvroVersion % scope)
)

val hadoopClientExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-api").
    exclude("javax.servlet", "servlet-api")

/*if it's a library the scope is "compile" since we want the transitive dependencies on the library
  otherwise we set up the scope to "provided" because those dependencies will be assembled in the "assembly"*/
lazy val assemblyDependenciesScope: String = if (isALibrary) "compile" else "provided"

lazy val hadoopDependenciesScope = if (isALibrary) "provided" else "compile"

libraryDependencies ++= Seq(
  sparkExcludes("org.apache.spark" %% "spark-core" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-sql" % sparkVersion % hadoopDependenciesScope),
//  sparkExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % hadoopDependenciesScope),

//  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % hadoopDependenciesScope),
//  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % hadoopDependenciesScope),
//  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % hadoopDependenciesScope),
//  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-applications-distributedshell" % hadoopVersion % hadoopDependenciesScope),
//  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % hadoopDependenciesScope)
) ++ assemblyDependencies(assemblyDependenciesScope)

//Trick to make Intellij/IDEA happy
//We set all provided dependencies to none, so that they are included in the classpath of root module
libraryDependencies := libraryDependencies.value.map{
  module =>
    if (module.configurations.equals(Some("provided"))) {
      module.copy(configurations = None)
    } else {
      module
    }
}

//http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath/21803413#21803413
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

//http://stackoverflow.com/questions/27824281/sparksql-missingrequirementerror-when-registering-table
fork := true

parallelExecution in Test := false

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(Defaults.itSettings: _*).
  enablePlugins(AutomateHeaderPlugin).
  enablePlugins(JavaAppPackaging).
  enablePlugins(AssemblyPlugin)

lazy val projectAssembly = (project in file("assembly")).
  settings(
    exportJars := true,
    test in assembly := {},
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(),
    assemblyMergeStrategy in assembly := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyJarName in assembly := s"$assemblyName-${version.value}.jar",
    libraryDependencies ++= assemblyDependencies("compile")
  ) dependsOn root settings (
  projectDependencies := {
    Seq(
      (projectID in root).value.excludeAll(ExclusionRule(organization = "org.apache.spark"),
        if (!isALibrary) ExclusionRule(organization = "org.apache.hadoop") else ExclusionRule())
    )
  })

mappings in Universal := {
  val universalMappings = (mappings in Universal).value
  val filtered = universalMappings filter {
    case (f, n) =>
      !n.endsWith(s"${organization.value}.${name.value}-${version.value}.jar")
  }

  val fatJar: File = new File(s"${System.getProperty("user.dir")}/target/scala-2.11/${name.value}_2.11-${version.value}.jar")
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

scriptClasspath ++= Seq(s"$assemblyName-${version.value}.jar")