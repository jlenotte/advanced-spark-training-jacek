// spark-shell
// spark-shell = Scala REPL + Spark-specific imports and vals
// IDE = interactive development environment for Spark
// We need data = Input Dataset
// Stay away from SC(SparkContext)
// You should always start your queries with spark

spark.sql("SELECT * FROM VALUES 1,2,3").show

// let's create a dataset
// datasets == dataframes

spark.range 
// [+ tab] to display all the methods

// A dataset == a data set
// Dataset[T]
// A Dataset object is like a table

val nums = spark.range(10)

nums.show

// We're about to use an implicit conversion from a Seq[(f1,f2,f3)] into a Dataset

Seq(
	(1, "one,two,three", "one"),
	(2, "four,one,five", "six")
).toDF("id", "Text1", "Text2")

// res4 : Spark will create a res[number] for any unnamed value
// We can reassign it to a val

val data = res4

// DataFrame =~= Dataset

data.show

// spark.range(1) 	-> proper answer to a certif question
// Seq(1,2,3).toDF 	-> another proper answer
// toDF and toDS are the basically the same, don't use toDS unless you know what you're doing. Stick to toDF
// Usually toDF gives better performance.
// flatMap() instead of explode(deprecated)
// ds.select(explode(split('words, " ")).as("word")) -> performance
// or flatMap()
// ds.flatMap.

data.printSchema

// :type 'c  -> Symbol literal!
// BY DEFAULT, ANY RELATIONAL NAME FOR DBs, COLUMN NAMES, ANYTHING DB RELATED IS NOT CASE-SENSITIVE

// This command will throw an error, because types don't match
data.select(split('Text1, ","), "*", '*).show

// This should work
data.select(split('Text1, ","), $"*", '*).show

// Remember that knownledge constrains you on the long run, refraining you from learning things that are new or unnatural to you

data.withColumn("words", split('Text1, ",")).show

data.select(split($"text1", ",") as "words").show

data.map(r => r.getLong("id") == 0).show	->
data.as[MyRow].map(mr => mr.id == 1).show	->
data.as[MyRow].map(_.id == 1).show			-> don't do this, ever. think twice, faster performance in detriment of safety, spark won't see what the bytecode is doing
data.select($"id" === 1 as "value").show 	-> can't prove type safety, not compile-time safe


val words = data. //complete me

words.withColumn("word", explode($"words")).groupBy("word").agg(collect_list("id") as "ids").show

val containsCol = expr(array_contains(split(text1,","), text2))

// In spark-sql 

spark-sql
show tables;

// In spark-shell

spark.sql("show tables")

// Every query result ends up as a DF
// .show will display the last result values
.show


spark.read.option("header", true).csv("../../people.csv").show

case class Person(id: Long, name: String, city: String)

spark.range(5).withColumn("group", when($"id" % 2 === 0, "EVEN").otherwise("ODD")).show

val nums = Seq(Seq(1,2,3)).toDF("nums")

nums.flatMap(n => n).foreach(println)
nums.flatMap(identity).foreach(println) // avoid println

nums.toDF.flatMap(identity).show 				// won't work with DF, use DS
nums.toDF.withColumn("exploded", explode('value)).show
nums.toDF.select(explode('value) as "value").show

// Reminder: STAY AWAY FROM DATASET API
// Always always always use DataFrame API
// Standard functions
// Do not write custom UDFs!!

spark.range(4).repartition(numPartitions = 4, 'id' % 4).queryExecution.toRdd.getNumPartitions
spark.range(4).repartition(numPartitions = 4).queryExecution.toRdd.getNumPartitions

val q = spark.range(4).repartition(numPartitions = 4).queryExecution.toRdd.getNumPartitions
q.explain(extended = true)

// S.M.A.C.K -> spark, mesos, akka, cassandra, kafka

spark.range(10).agg(max('id) as "max").show
spark.range(10).select(max('id)).show

val nums = spark.range(10)
nums.groupBy('id % 2 as "group").agg(sum('id) as "sum")

val data = spark.range(10).withColumn("group", 'id % 2)

data.groupBy("group").agg(max('id), min('id)).show

spark.range(10).withColumn("g", 'id % 2).show
spark.range(10).withColumn("g", 'id % 2).groupBy("g").agg(max('id) as "max").show

Seq((1, "one"), (2, "two")).toDF("id", "name")

spark.read.text("exercise.txt").show(truncate = false)
val data = spark.read.text("exercise.txt").filter(_.startsWith("|")).map(_.substring(1)).map(_.split('|')).show
data.head
data.take(1)


// : _* is Scala's way to flatten an Array
input.select((0 until 3).map(n => 'value(n) as s"$n"): _*).show

val headers = data.head.map(_.tim)
headers.foldLeft(spark.emptyDataset[Long]) { (header, ds) => ds.withColumn(header, ds(header)) }

val data = spark.read.format("csv").option("header", "true").option("separator", ",").load("test3.csv")

data.withColumn("running_total", sum('items_sold) over departmentByTimeAsc).show

data.select('words).map(w => w.toString as $"solution").show

// Transform array of strings into strings
data.withColumn("solution", concat('words(0), lit(" "), 'word(1))).show
data.map(ss => (ss, ss.mkString(" "))).toDF(words.columns.head, "solution").show
// or use correct builtin function 'concat_ws'


// Streaming CSV datasets
  val spark = SparkSession
    .builder()
    .appName("Spark Structured Streaming App")
    .getOrCreate()

  val in = spark.readStream
    .format("csv")
    .load

  val q = in
    .writeStream
    .format("console")
    .option("truncate", false)
    .start

  q.awaitTermination


// linux zookeeper start
./bin/zookeper-server-start.sh config/zookeeper.properties
// linux kafka start
./bin/kafka-server-start.sh config/server.properties

// linux producer
./bin/kafka-console-poducer.sh --broker-list :9092 --topic ovh-input
// windows producer
c:/path/kafka-console-producer.bat --broker-list :9092 --topic ovh-input

// linux consumer
./bin/kafka-console-consumer.sh --topic ovh-input --bootstrap-server :9092
// windows consumer
kafka-console-consumer.bat --topic ovh-input --bootstrap-server :9092