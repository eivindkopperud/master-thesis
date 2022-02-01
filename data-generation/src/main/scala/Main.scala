import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, last, when}
import org.json4s.JsonDSL.int2jvalue





object Main extends App {
  val spark = SparkSession.builder.master("local").getOrCreate
  var sc = spark.sparkContext

  /*
       Tried with windows on RDDs first, might try again now that I fixed the stupid sbt issues:)

  val events = "src/main/resources/fb-messages.csv"
  val eventRDD = CsvUtils
    .textFileToRDDCaseClass(events, sc)
    .asInstanceOf[RDD[TemporalEvent[BigDecimal]]]
  */

  val df = spark.read.csv("src/main/resources/fb-messages.csv").toDF("from","to","time")
  val w = Window.orderBy("from", "to", "time")
  var leadDf = df
    .withColumn("previous", when(col("to") === lag("to", 1).over(w), lag("time", 1).over(w)).otherwise(""))
    .withColumn("closeness", when(col("previous") === "", "" ).otherwise(col("time") - col("previous")))
    .withColumn("id", when(col("closeness") === "", functions.monotonically_increasing_id()))
    .withColumn("edge id", last(col("id"), ignoreNulls = true).over(w.rowsBetween(Window.unboundedPreceding, 0)))
    // https://stackoverflow.com/questions/67888618/get-min-and-max-from-values-of-another-column-after-a-groupby-in-pyspark
    .groupBy("edge id").agg(functions.min("time").alias("from time"), functions.max("time").alias("to time"))

  leadDf.show()


}
