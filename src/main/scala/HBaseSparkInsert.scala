import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

object HBaseSparkInsert {

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


    val data = Seq(Employee("4","Abby","Smith","K","3456 main", "Orlando","FL","45235"),
      Employee("5","Amaya","Williams","L","123 Orange","Newark", "NJ", "27656"),
      Employee("6","Alchemy","Davis","P","Warners","Sanjose","CA", "34789"))

    val spark: SparkSession = SparkSession.builder()
      .appName("HBaseSparkInsert")
      .getOrCreate()

//    val config = new HBaseConfiguration()
    val hbaseContext = new HBaseContext(spark.sparkContext, HBaseConfiguration.create())

    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).toDF

    df.write.options(
//      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }
}