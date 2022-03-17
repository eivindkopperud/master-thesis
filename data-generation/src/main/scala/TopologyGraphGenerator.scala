import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, last, when}
import org.apache.spark.sql.{SparkSession, functions}

import java.time.Instant

final case class TimeInterval(start:Instant, stop:Instant)

object TopologyGraphGenerator {

  def generateGraph(
                     spark: SparkSession,
                     threshold: BigDecimal,
                     filePath: String="src/main/resources/fb-messages.csv",
                     delimiter: String=","
                   ):Graph[Long, TimeInterval] = {
    val window = Window.orderBy("from", "to", "time")
    val df = spark.read.option("delimiter", delimiter).csv(filePath)
      .toDF("from", "to", "time")
      .withColumn("previous", when(col("to") === lag("to", 1).over(window), lag("time", 1).over(window)).otherwise(""))
      .withColumn("closeness", when(col("previous") === "", "").otherwise(col("time") - col("previous")))
      .withColumn("id", when(col("closeness") === "" || col("closeness") > threshold, functions.monotonically_increasing_id()))
      .withColumn("edgeId", last(col("id"), ignoreNulls = true).over(window.rowsBetween(Window.unboundedPreceding, 0)))
    val tempDf = df.groupBy("edgeId").agg(functions.min("time").alias("from time"), functions.max("time").alias("to time"))
    val leadDf = df.as("self1").join(tempDf.as("self2"), col("self1.id") === col("self2.edgeId"), "inner").select("from", "to", "from time", "to time")
    val vertices: RDD[(Long, Long)] = leadDf
      .select("from")
      .distinct
      .rdd.map(row => row.getAs[String]("from").toLong)
      .map(long => (long, long))

    val edges: RDD[Edge[TimeInterval]] = leadDf
      .select("from", "to", "from time", "to time")
      .rdd.map(
      row =>
        Edge(row.getAs[String]("from").toLong, row.getAs[String]("to").toLong,
          TimeInterval(Instant.ofEpochSecond(row.getAs[Long]("from time")), Instant.ofEpochSecond(row.getAs[Long]("to time")))))

    Graph(vertices, edges)
  }
}
