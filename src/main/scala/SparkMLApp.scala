object SparkMLApp extends App {

  import java.io.File
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.feature.Tokenizer
  import org.apache.spark.ml.feature.HashingTF
  import org.apache.spark.ml.Pipeline
  import org.apache.spark.ml.PipelineModel
  import spark.implicits._
  import org.apache.spark.sql.functions.when
  import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
  import org.apache.spark.ml.param.ParamMap
  import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

  // FIX Use scopt for command-line options
  // EXTRA: Persist the model and reuse

  val inputCol            = args(0) // "body"
  val trainingDataCsv     = args(1)
  val prodDataCsv         = args(2)
  val logReg              = new LogisticRegression
  val tok                 = new Tokenizer().setInputCol(inputCol)
  val hashTF              = new HashingTF().setInputCol(tok.getOutputCol).setOutputCol(logReg.getFeaturesCol)
  val pipeline            = new Pipeline().setStages(Array(tok, hashTF, logReg))
  val spark               = SparkSession.builder().getOrCreate()
  val opts                = Map("header" -> true.toString, "inferSchema" -> true.toString)
  val trainingData        = spark.read.options(opts).csv(trainingDataCsv)
  val prodData            = spark.read.options(opts).csv(prodDataCsv)
  val modelDir            = "classificaton-model"
  val model               = if (new File(modelDir).exists()) {
                              PipelineModel.load(modelDir)
                            } else {
                              val m = pipeline.fit(trainingData)
                              m.write.overwrite().save(modelDir)
                              m
                            }
  val predictions         = model.transform(prodData)
  val status              = when('predictions === 0, "OK").otherwise("SPAM").as("status")
  val evaluator           = new BinaryClassificationEvaluator
  val paramGrid: Array[ParamMap]
                          = new ParamGridBuilder().addGrid(logReg.elasticNetParam, Seq(0,0.5,1)).build
  val cv                  = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
  val bestModel           = cv.fit(trainingData)
  val bm                  = bestModel
    .bestModel
    .asInstanceOf[PipelineModel]
    .parent
    .asInstanceOf[Pipeline]
    .getStages(2)
    .asInstanceOf[LogisticRegression]
  val elasticNetParams    = bestModel
    .bestModel
    .asInstanceOf[PipelineModel]
    .parent
    .asInstanceOf[Pipeline]
    .getStages(2)
    .asInstanceOf[LogisticRegression]

  println(s">>> PErsisting the model to $modelDir")
  println(">>> >>> Best model params")
  println(s">>> >>> >>> ElasticNetParams = $elasticNetParams")

  predictions.select('body, status).show

}

