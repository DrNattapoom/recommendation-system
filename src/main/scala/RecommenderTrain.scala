import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object RecommenderTrain {

  def main(args: Array[String]): Unit = {

    // initialize Spark
    val conf = new SparkConf()
      .setAppName("RecommenderTrain")
      .setMaster("local")
    val sc = new SparkContext(conf)

    println("master = " + sc.master)

    // set up logger
    setLogger()

    // set up data path
    val dataPath = "/path/to/u.data"
    val modelPath = "/path/to/ALSmodel"
    val checkpointPath = "/path/to/checkpoint/"
    // set checkpoint directory to avoid stackoverflow error
    sc.setCheckpointDir(checkpointPath)

    // process data
    val ratingsRDD: RDD[Rating] = processData(sc, dataPath)
    // add checkpoint to avoid stackoverflow error
    ratingsRDD.checkpoint()

    // train the model
    val model: MatrixFactorizationModel = ALS.train(ratingsRDD, 2, 10, 0.01)

    // save the model
    saveModel(sc, model, modelPath)
    sc.stop()

  }

  def setLogger(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  def processData(sc: SparkContext, dataPath: String): RDD[Rating] = {
    // create Spark RDD from data
    val rawRDD: RDD[String] = sc.textFile(dataPath)
    // read first three columns and turn them into Rating object
    val ratingsRDD: RDD[Rating] = rawRDD.map(line =>
      line.split("\t") match {
        case Array(user, item, rate, _) => Rating(user.toInt, item.toInt, rate.toDouble)
      }
    )
    // return processed data as Spark RDD
    ratingsRDD
  }

  def saveModel(context: SparkContext, model: MatrixFactorizationModel, modelPath: String): Unit = {
    try {
      model.save(context, modelPath)
    } catch {
      case e: Exception => println("ERROR: unable to save the model")
    }
    finally {}
  }

}
