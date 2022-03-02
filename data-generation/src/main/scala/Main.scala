import TopologyGraphGenerator.generateGraph
import org.apache.spark.sql.SparkSession


object Main extends App {
  val spark = SparkSession.builder.master("local").getOrCreate
  val graph = generateGraph(spark, 250)

}
