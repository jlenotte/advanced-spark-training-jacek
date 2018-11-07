import org.apache.spark.sql.SparkSession

object LocalRunner extends App {

    SparkSession
            .builder()
            .master("local[*]")
            .getOrCreate()
    SparkSqlApp.main(args)
}
