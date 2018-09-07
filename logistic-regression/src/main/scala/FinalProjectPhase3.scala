package LogisticReg

import java.io.{File, PrintWriter}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FinalProjectPhase3 {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("FinalProjectPhase3").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val resource = this.getClass.getClassLoader.getResource("FinalProjectPhase3/cleanData.csv")
    val uri: String = new File(resource.toURI).getPath
    val reducedData: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    .load(uri).toDF().na.fill(0)

    reducedData.show()

    val featureCols = Array("Score", "BodyLength", "CommentCount", "TimeSinceCreation",
      "SumCommentScore", "UserCreationDate", "Age", "AboutMeLength", "Explainer", "Nice Question")

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    // Normalize Data
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    // Training and Test data
    val Array(trainingData: DataFrame, testData: DataFrame) = reducedData.randomSplit(Array(0.8, 0.2))
    trainingData.cache()
    testData.cache()

    // Make and Run model
    val lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("AcceptedByOriginator")
    val pipeline = new Pipeline().setStages(Array(assembler, scaler, lr))
    val model = pipeline.fit(trainingData)
    val result = model.transform(testData)

    // Get correlations and coefficients
    val modelTrained = model.stages(2).asInstanceOf[LogisticRegressionModel]
    val Row(corrCoef: Matrix) = Correlation.corr(result, "scaledFeatures").head

    // Evaluate model
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("AcceptedByOriginator")
      .setRawPredictionCol("rawPrediction")
    val accuracy = evaluator.evaluate(result)
    val lp = result.select(result("AcceptedByOriginator").alias("label"), result("prediction"))
    val trueN = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val trueP = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()

    val pw = new PrintWriter(new File("test.txt"))
    pw.write("Features: " + featureCols.mkString(",") + "\n")
    pw.write("Coefficients: " + modelTrained.coefficients + "\n")
    pw.write("Intercept: " + modelTrained.intercept + "\n")
    pw.write("Pearson correlation matrix:\n" + corrCoef.toString(featureCols.length, Int.MaxValue))
    pw.write("Accuracy" + accuracy + "\n")
    pw.write("TrueN" + trueN + "\n")
    pw.write("TrueP" + trueP + "\n")
    pw.write("FalseN" + falseN + "\n")
    pw.write("FalseP" + falseP + "\n")
    pw.close()

    println("Features: " + featureCols.mkString(",") + "\n")
    println("Coefficients:" + modelTrained.coefficients + "\n")
    println("Intercept:" + modelTrained.intercept + "\n")
    println("Pearson correlation matrix:\n" + corrCoef.toString(featureCols.length, Int.MaxValue))
    println("Accuracy" + accuracy + "\n")
    println("TrueN" + trueN + "\n")
    println("TrueP" + trueP + "\n")
    println("FalseN" + falseN + "\n")
    println("FalseP" + falseP + "\n")
    sc.stop()
  }
}
