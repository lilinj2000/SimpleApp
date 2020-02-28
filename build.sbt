name := "SimpleApp"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.1.0-cdh6.3.1"
libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.1.0-cdh6.3.1"
//libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.3-2.4-s_2.11"