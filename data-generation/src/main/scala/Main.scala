import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, last, when}
import org.apache.spark.sql.{SparkSession, functions}


object Main extends App {
  val spark = SparkSession.builder.master("local").getOrCreate
  val threshold = 250.0
  val window = Window.orderBy("from", "to", "time")
  val df = spark.read.csv("src/main/resources/fb-messages.csv")
    .toDF("from", "to", "time")
    .withColumn("previous", when(col("to") === lag("to", 1).over(window), lag("time", 1).over(window)).otherwise(""))
    .withColumn("closeness", when(col("previous") === "", "").otherwise(col("time") - col("previous")))
    .withColumn("id", when(col("closeness") === "" || col("closeness") > threshold, functions.monotonically_increasing_id()))
    .withColumn("edgeId", last(col("id"), ignoreNulls = true).over(window.rowsBetween(Window.unboundedPreceding, 0)))
  val tempDf = df.groupBy("edgeId").agg(functions.min("time").alias("from time"), functions.max("time").alias("to time"))
  val leadDf = df.as("self1").join(tempDf.as("self2"), col("self1.id") === col("self2.edgeId"), "inner").select("from", "to", "from time", "to time")

  val vertices: RDD[(VertexId, Long)] = leadDf
    .select("from")
    .distinct
    .rdd.map(row => row.getAs[String]("from").toLong)
    .zipWithIndex // associate a long index to each vertex
    .map(_.swap)

  val edges: RDD[Edge[(BigDecimal,BigDecimal)]] = leadDf
    .select("from", "to", "from time", "to time")
    .rdd.map(
    row =>
      Edge(row.getAs[String]("from").toLong, row.getAs[String]("to").toLong,
        (row.getAs[BigDecimal]("from time"), row.getAs[BigDecimal]("to time"))))

  val graph = Graph(vertices, edges)
  println(graph.numEdges, graph.numVertices)
}
