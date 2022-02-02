import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, last, when}


object Main extends App {
  val spark = SparkSession.builder.master("local").getOrCreate
  var sc = spark.sparkContext
  val threshold = 500.0
  val df = spark.read.csv("src/main/resources/fb-messages.csv").toDF("from","to","time")
  val window = Window.orderBy("from", "to", "time")
  var leadDf = df
    .withColumn("previous", when(col("to") === lag("to", 1).over(window), lag("time", 1).over(window)).otherwise(""))
    .withColumn("closeness", when(col("previous") === "", "" ).otherwise(col("time") - col("previous")))
    .withColumn("id", when(col("closeness") === "" || col("closeness") > threshold, functions.monotonically_increasing_id()))
    .withColumn("edgeId", last(col("id"), ignoreNulls = true).over(window.rowsBetween(Window.unboundedPreceding, 0)))
    val tempDf = leadDf.groupBy("edgeId").agg(functions.min("time").alias("from time"), functions.max("time").alias("to time"))
    val leadDf2 = leadDf.as("self1").join(tempDf.as("self2"), col("self1.id") === col("self2.edgeId"), "inner").select("from", "to", "from time", "to time")

  leadDf2.show()
}
