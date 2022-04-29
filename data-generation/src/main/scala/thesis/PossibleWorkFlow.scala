package thesis

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import thesis.DataSource.FbMessages
import thesis.DistributionType.UniformType
import thesis.LTSV.{RDDLogTSVMethod, readFromFile}
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
    val logger = LoggerFactory.getLogger("PossibleWorkFlow")
    val start = System.nanoTime()

    val usePrevious = true // Set to true if you want to be deterministic in the runs
    val logs = if (usePrevious) {
      logger.warn("Using previous logs")
      sc.parallelize(readFromFile("previous_run.tsv"))
    } else {
      logger.warn("Generate inital graph from dataset")
      val graph = generateGraph(2, dataSource = FbMessages)

      logger.warn("Augment the dataset with updates")
      val g = addGraphUpdateDistribution(graph, UniformType(0, 2))

      logger.warn("Generate updates and output as logs")
      val logs = generateLogs(g)

      // If you want deterministic debugging, write the logs to a file and run using them
      logger.warn("Writing logs to file")
      logs.serializeLogs(filename = "previous_run.tsv") // Write to file just in case
      logs
    }
    logger.warn("Generate temporal model")
    val snapshotModel = SnapshotDeltaObject.create(logs, SnapshotIntervalType.Count(10000))

    println(s"Number of vertices in the first snapshot ${snapshotModel.graphs.head.graph.vertices.count()}")
    println(s"Number of logs ${logs.count()}")

    val landyModel = Landy(logs)
    //TODO benchmark queries
    val graphAtTime = snapshotModel.snapshotAtTime(2012)
    println(s"Graph at time 2012 has ${graphAtTime.vertices.count()} vertices and ${graphAtTime.edges.count()} edges")

    val graphAtTimeLandy = landyModel.snapshotAtTime(2013)
    println(s"Graph at time 2012 has ${graphAtTimeLandy.vertices.count()} vertices and ${graphAtTimeLandy.edges.count()} edges")

    // Timing of whole process
    val end = System.nanoTime()
    logger.warn(s"Time taken: ${NANOSECONDS.toSeconds(end - start)} s")
  }

}
