import org.apache.spark.sql.SparkSession

object SparkKafkaApp extends App {


  val spark = SparkSession.builder().getOrCreate()


  val data= spark.readStream
    .format("kafka")
    .option("subscribe", "ovh")
    .option("kafka.bootstrap.servers", args(1))
    .load // <--- this returns a DatFrame


  // Tranformations
  // 1. Extracting values
  // 2. Casting to string type
  import spark.implicits._
  import org.apache.spark.sql.types.StringType
  val values = data.select('value cast StringType)


  // Output goes to console
  val toConsole = values
    .writeStream
    .format("console")
    .option("truncate", false)
    .start

  // Output goes to Kafka topic
  val toKafkaTopic = values
    .writeStream
    .format("kafka")
    .option("checkpointLocation", "checkpoint-location")
    .option("topic", args(2))
    .start

  spark.streams.awaitAnyTermination()
}


// in order to be able to specify this option with spark-sumbit,
// change this:
// .options("subscribe", args(0))
