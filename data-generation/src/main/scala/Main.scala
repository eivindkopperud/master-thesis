import TopologyGraphGenerator.generateGraph
import UpdateDistributions.addVertexUpdateDistribution
import org.apache.spark.sql.SparkSession


object Main extends App {
  val spark = SparkSession.builder.master("local").getOrCreate
  val graph = generateGraph(spark, 1, filePath = "src/main/resources/reptilia-tortoise-network-sl.csv", delimiter = " ")
  spark.sparkContext.setLogLevel("WARN")
  //val anothergraph = genLogNormalGraph(spark.sparkContext, 1000, 100)
  val withDistribution = addVertexUpdateDistribution(spark.sparkContext, graph, UpdateDistributionMode.Uniform, DistributionType.LogNormal, 5, 0.5)
  withDistribution.vertices.foreach(x => println(x._1, x._2))

}