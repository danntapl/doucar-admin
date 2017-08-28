/* # Deploying Models and Pipelines in Production */

/* **Note**: // comments do not render Markdown. */

/* In this module we demonstrate how to load transformers, estimators, and
pipelines that were saved from Python into Scala. */


/* ## Setup */

/* In a Scala session, the SparkSession is created for us: */
spark

/* Read the enhanced ride data from HDFS: */
val rides = spark.read.parquet("/duocar/joined_all/")

/* **Note:** We are reading a different dataset (joined_all rather than
joined). */


/* ## Preprocess the data */

/* Convert `star_rating` from integer to double: */
val pre2 = rides.withColumn("star_rating_double", rides("star_rating").cast("double"))

/* Rename the preprocessed DataFrame: */
val preprocessed = pre2

/* **Note:** We could implement these preprocessing steps in a `SQLTransformer`
and pipeline the entire workflow. */
  
/* ## Transform the data */

/* ### Binarize star_rating */

/* Read the
[Binarizer](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.Binarizer)
instance from HDFS: */
import org.apache.spark.ml.feature.Binarizer
val binarizer = Binarizer.load("myduocar/binarizer")

/* Inspect the Binarizer instance: */
binarizer.getInputCol
binarizer.getOutputCol
binarizer.getThreshold

/* Apply the Binarizer: */
val binarized = binarizer.transform(preprocessed)

/* Verify transformation */
binarized.groupBy("star_rating", "star_rating_binarized").count().orderBy("star_rating").show()
