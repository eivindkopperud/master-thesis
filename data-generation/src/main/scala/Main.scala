import GeneratedGraph.genLogNormalGraph
import UpdateDistributions.add_log_normal_update_distr_vertex
import org.apache.spark.sql.SparkSession


object Main extends App {
  val spark = SparkSession.builder.master("local").getOrCreate
  //val graph = generateGraph(spark, 250)
  spark.sparkContext.setLogLevel("WARN")
  val anothergraph = genLogNormalGraph(spark.sparkContext, 1000, 100)
  add_log_normal_update_distr_vertex(anothergraph, mu = 5, sigma = 0.5)

}