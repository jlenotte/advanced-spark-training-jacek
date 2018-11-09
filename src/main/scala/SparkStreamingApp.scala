import org.apache.spark.sql.SparkSession

object SparkStreamingApp extends App {


  case class Person(id: Long, name: String)
  import org.apache.spark.sql.Encoders


  val schema = Encoders.product[Person].schema


  val spark = SparkSession
    .builder()
    .appName("Spark Structured Streaming App")
    .master("local[*]")
    .getOrCreate()


  val in = spark.readStream
    .format("csv")
    .option("header", true)
    .schema(schema)
    .load
    .writeStream
    .format("console")
    .option("truncate", false)
    .start


  in.awaitTermination

}

