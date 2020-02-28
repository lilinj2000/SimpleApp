/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
//import org.apache.log4j.{Level,Logger}

object SimpleApp {
  def main(args: Array[String]) {
    //    Logger.getRootLogger.setLevel(Level.WARN)
    val logFile = "test.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}