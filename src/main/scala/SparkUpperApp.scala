import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}

object SparkUpperApp extends App {

  val spark = SparkSession
    .builder()
    .appName("UpperApp")
    .getOrCreate()


  import org.apache.spark.sql.functions.{lit, upper}
  import spark.implicits._

  val data = spark
    .range(5)
    .withColumn("name", lit("oNe"))
    .withColumn("NAME", upper('name))

  val dataJ = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("separator", ";")
    .load("c:/bla/bla/file.csv")


  val dates = Seq(
    "08/11/2018",
    "12/11/2018",
    "21/12/2018"
  ).toDF("date_string")

  def datediff(current_date: String, name: ColumnName) = {

  }


}


