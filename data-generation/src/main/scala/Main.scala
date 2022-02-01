import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}





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
  val leadDf = df
  .withColumn("previous", when(col("to") === lag("to", 1).over(w), lag("time", 1).over(w)).otherwise(""))
  .withColumn("closeness", when(col("previous") === "", "" ).otherwise(col("time") - col("previous")))
  leadDf.show()


}
