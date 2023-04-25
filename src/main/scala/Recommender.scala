import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object Recommender {

  def main(args: Array[String]) = {

    if (args.length != 2) {
      println("ERROR: 2 arguments are needed: ")
      printHelp()
      sys.exit(0)
    }
    // recommend type
    val recommendType = args(0)
    // input id
    val inputID = args(1)

    // initialize Spark
    val conf = new SparkConf()
      .setAppName("Recommender")
      .set("spark.ui.showConsoleProgress", "false")
      .setMaster("local")
    val sc = new SparkContext(conf)

    println("master = " + sc.master)

    // set up logger
    setLogger()

    // set up data path
    val dataPath = "/path/to/u.item"
    val modelPath = "/path/to/ALSmodel"
    val checkpointPath = "/path/to/checkpoint/"
    // set checkpoint directory to avoid stackoverflow error
    sc.setCheckpointDir(checkpointPath)

    // process data
    val movieTitle: RDD[(Int, String)] = processData(sc, dataPath)
    // add checkpoint to avoid stackoverflow error
    movieTitle.checkpoint()

    // load model
    val model = loadModel(sc, modelPath)

    // recommend
    recommend(model, movieTitle, recommendType, inputID)

    sc.stop()

  }

  private def printHelp(): Unit = {
    println("Please provide the following arguments:")
    println("1. recommend type: ")
    println("\t'--u' for recommending movies to a user")
    println("\t'--m' for predicting which users may be interested in a movie")
    println("2. id: ")
    println("\tuserID (after '--u')")
    println("\tmovieID (after '--m')")
  }

  def setLogger(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  def processData(sc: SparkContext, dataPath: String): RDD[(Int, String)] = {
    println("Loading Data......")
    // reads data from dataPath into Spark RDD.
    val itemRDD: RDD[String] = sc.textFile(dataPath)
    // only takes in first two fields (movieID, movieName).
    val movieTitle: RDD[(Int, String)] = itemRDD.map(line => line.split("\\|")).map(x => (x(0).toInt, x(1)))
    // return movieID->movieName map as Spark RDD
    movieTitle
  }

  def loadModel(sc: SparkContext, modelPath: String): Option[MatrixFactorizationModel] = {
    try {
      val model: MatrixFactorizationModel = MatrixFactorizationModel.load(sc, modelPath)
      Some(model)
    } catch {
      case e: Exception => {
        println("ERROR: unable to load the model")
        None
      }
    } finally {}
  }

  def recommend(model: Option[MatrixFactorizationModel], movieTitle: RDD[(Int, String)], arg1: String, arg2: String) = {
    if (arg1 == "--u") {
      recommendMovies(model.get, movieTitle, arg2.toInt)
    }
    if (arg1 == "--m") {
      recommendUsers(model.get, movieTitle, arg2.toInt)
    }
  }

  def recommendMovies(model: MatrixFactorizationModel, movieTitle: RDD[(Int, String)], userID: Int) = {
    val recommended = model.recommendProducts(userID, 10)
    println(s"The following movies are recommended for user ${userID.toString}:")
    recommended.foreach(entry => println(s"user: ${entry.user}, recommended movie: ${movieTitle.lookup(entry.product).mkString}, rating: ${entry.rating}"))
  }

  def recommendUsers(model: MatrixFactorizationModel, movieTitle: RDD[(Int, String)], movieID: Int) = {
    val recommeded = model.recommendUsers(movieID, 10)
    println(s"The following users may be interested in movie ${movieID.toString}:")
    recommeded.foreach(entry => println(s"movie: ${movieTitle.lookup(entry.product).mkString}, recommended user: ${entry.user}, rating: ${entry.rating}"))
  }

}
