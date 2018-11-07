import org.apache.spark.sql.SparkSession

object SparkSqlApp extends App {

    println("Hello OVH")

    val spark = SparkSession
        .builder
        // .master("local[*]") // how to be able to  run this application from IDEA without hardcoding local[*]
        .getOrCreate()

    spark.range(10).show()

    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("separator", ",")
      .load("ton/chemin/vers/csv")


}
