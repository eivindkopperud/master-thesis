package thesis

import org.slf4j.LoggerFactory
import thesis.SparkConfiguration.getSparkSession
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}

import scala.concurrent.duration.NANOSECONDS

object PossibleWorkFlow {

  def run(): Unit = {

    // Init spark stuff
    val spark = getSparkSession
    val sc = spark.sparkContext
    sc.setLogLevel("WARN") // I can't get the log4j.properties file to have any effect
    val logger = LoggerFactory.getLogger("PossibleWorkFlow")
    val start = System.nanoTime()

    logger.warn("Generate inital graph from dataset")
    val graph = generateGraph(spark, 1)

    logger.warn("Augment the dataset with updates")
    val g = addGraphUpdateDistribution(sc, graph)

    logger.warn("Generate updates and output as logs")
    val logs = generateLogs(g)

    logger.warn("Generate temporal model")
    val snapshotModel = SnapshotDeltaObject.create(logs, SnapshotIntervalType.Count(10))

    println(s"Number of vertices in the first snapshot ${snapshotModel.graphs.head.vertices.count()}")
    println(s"Number of logs ${logs.count()}")

    //TODO generate Landy Model


    //TODO benchmark queries

    // Timing of whole process
    val end = System.nanoTime()
    logger.warn(s"Time taken: ${NANOSECONDS.toSeconds(end - start)} s")
  }

}
