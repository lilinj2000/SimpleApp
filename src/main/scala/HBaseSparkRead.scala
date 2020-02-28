import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

object HBaseSparkRead {

  case class Employee(key: String, fName: String, lName: String,
                      mName:String, addressLine:String, city:String,
                      state:String, zipCode:String)

  def main(args: Array[String]): Unit = {

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"employee"},
         |"rowkey":"key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"key", "type":"string"},
         |"fName":{"cf":"person", "col":"firstName", "type":"string"},
         |"lName":{"cf":"person", "col":"lastName", "type":"string"},
         |"mName":{"cf":"person", "col":"middleName", "type":"string"},
         |"addressLine":{"cf":"address", "col":"addressLine", "type":"string"},
         |"city":{"cf":"address", "col":"city", "type":"string"},
         |"state":{"cf":"address", "col":"state", "type":"string"},
         |"zipCode":{"cf":"address", "col":"zipCode", "type":"string"}
         |}
         |}""".stripMargin

    val spark: SparkSession = SparkSession.builder()
      .appName("HBaseSparkRead")
      .getOrCreate()

    val hbaseContext = new HBaseContext(spark.sparkContext, HBaseConfiguration.create())

    import spark.implicits._

    // Reading from HBase to DataFrame
    val hbaseDF = spark.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.hadoop.hbase.spark")
      .load()

    //Display Schema from DataFrame
    hbaseDF.printSchema()

    //Collect and show Data from DataFrame
    hbaseDF.show()

    //Applying Filters
//    hbaseDF.filter($"key" === "4" && $"state" === "FL")
//      .select("key", "fName", "lName")
//      .show()

//    //Create Temporary Table on DataFrame
//    hbaseDF.createOrReplaceTempView("employeeTable")
//
//    //Run SQL
//    spark.sql("select * from employeeTable where fName = 'Amaya' ").show

  }
}