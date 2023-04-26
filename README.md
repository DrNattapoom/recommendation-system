# Recommendation System
Collaborative Filtering Machine Learning using Alternating Least Square (ALS) Algorithm with Scala, Spark, and Hadoop

# Repository Files
```
├── project
│   └── build.properties
├── src
│   └── main
│       └── scala
│           ├── META-INF
│           │   └── MANIFEST.MF
│           ├── Recommender.scala
│           └── RecommenderTrain.scala
├── .gitignore
├── README.md
└── build.sbt
```

# Project Setup
```
Ubuntu 20.04
JDK 11.0.13
Scala 2.12.11
Spark 3.2.0
Hadoop 3.2.2
```

# Dataset
The dataset used in this project is the <b>ml-100k</b> dataset taken from <a href="https://grouplens.org/datasets/movielens/">GroupLens Research</a> at the University of Minnesota.

# Run
To train the model, please execute the following command.
```
spark-submit --class RecommenderTrain --master yarn --deploy-mode client /path/to/package.jar
```
Please make sure HDFS and YARN services are running and properly configured.

<b> Note: </b> `package.jar` <b> must be </b> generated first.

The trained model can be used to  

1. Recommend movies to a user with the specified userID 
```
spark-submit --class Recommender --master yarn --deploy-mode client /path/to/package.jar --u <userID>
```

2. Predict which users may be interested in a movie with the specified movieID
```
spark-submit --class Recommender --master yarn --deploy-mode client /path/to/package.jar --m <movieID>
```

<b> Note: </b> Please make sure to train the model first.

P.S. If you do not want to set up Scala and Apahce Hadoop, please check out <a href = "https://github.com/DrNattapoom/recommendation-system-pyspark">recommendation-system-pyspark</a> which is an alternative implementation that uses Python and a Databricks cluster instead.
