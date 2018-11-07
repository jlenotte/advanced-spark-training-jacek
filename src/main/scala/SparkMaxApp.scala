import org.apache.spark.sql.SparkSession

object SparkMaxApp extends App {

  val spark = SparkSession
    .builder().getOrCreate()

  import spark.implicits._

  spark.range(10).agg(max('id) as "max").show
  spark.range(10).select(max('id)).show
}
