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

val containsCol = expr(array_contains(split(text1,","), text2)")


----------------------------------------------------

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
