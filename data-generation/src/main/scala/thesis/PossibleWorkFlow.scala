package thesis

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import thesis.SparkConfiguration.getSparkSession
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}

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

    logger.warn("Generate temporal model")
    val snapshotModel = SnapshotDeltaObject.create(logs, SnapshotIntervalType.Count(10))

    println(s"Number of vertices in the first snapshot ${snapshotModel.graphs.head._1.vertices.count()}")
    println(s"Number of logs ${logs.count()}")

    //TODO generate Landy Model


    //TODO benchmark queries

    // Timing of whole process
    val end = System.nanoTime()
    logger.warn(s"Time taken: ${NANOSECONDS.toSeconds(end - start)} s")
  }

}
