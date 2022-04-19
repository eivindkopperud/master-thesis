package thesis

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import thesis.Entity.{EDGE, VERTEX}
import thesis.SparkConfiguration.getSparkSession
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import utils.TimeUtils._

import scala.concurrent.duration.NANOSECONDS

object PossibleWorkFlow {

  def run(): Unit = {

    // Init spark stuff
    // https://www.theguardian.com/info/developer-blog/2016/dec/22/parental-advisory-implicit-content
    implicit val spark: SparkSession = getSparkSession
    implicit val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN") // I can't get the log4j.properties file to have any effect
    val logger = LoggerFactory.getLogger("PossibleWorkFlow")
    val start = System.nanoTime()

    logger.warn("Generate inital graph from dataset")
    val graph = generateGraph(1)

    logger.warn("Augment the dataset with updates")
    val g = addGraphUpdateDistribution(graph)

    logger.warn("Generate updates and output as logs")
    val logs = generateLogs(g)

    logs.filter(_.entity match { case VERTEX(_) => true case EDGE(_, _) => false }).foreach(println)

    logger.warn("Generate temporal model")
    val snapshotModel = SnapshotDeltaObject.create(logs, SnapshotIntervalType.Count(5))

    println(s"Number of vertices in the first snapshot ${snapshotModel.graphs.head.graph.vertices.count()}")
    println(s"Number of logs ${logs.count()}")

    //TODO generate Landy Model


    //TODO benchmark queries
    val graphAtTime = snapshotModel.snapshotAtTime(2012)
    println(s"Graph at time 2012 has ${graphAtTime.vertices.count()} vertices and ${graphAtTime.edges.count()} edges")

    // Timing of whole process
    val end = System.nanoTime()
    logger.warn(s"Time taken: ${NANOSECONDS.toSeconds(end - start)} s")
  }

}
