package benchmarks

import org.apache.spark.graphx.Graph
import org.slf4j.Logger
import thesis.DataSource.ContactsHyperText
import thesis.DistributionType.UniformType
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{DataSource, DistributionType, Interval, Landy}
import utils.Dictionary
import utils.TimeUtils.secondsToInstant

import scala.collection.immutable.HashMap


class LandyDirectNeighbours(iterationCount: Int = 3,
                            customColumn: String = "Landy direct neighbours"
                           ) extends QueryBenchmark(iterationCount, customColumn) {
  val logger: Logger = getLogger

  // Relevant parameters for this benchmark
  val config: Dictionary = Dictionary(HashMap(
    "threshold" -> 40,
    "datasource" -> ContactsHyperText,
    "distributionType" -> ((iteration: Int) => UniformType(1, 2 * (iteration + 1))),
    "timestamp" -> 1082148639L,
    "intervalDelta" -> 1000L,
    "vertexId" -> 37L
  ))

  lazy val graph: Graph[Long, Interval] = {
    val threshold = config[Int]("threshold")
    generateGraph(threshold, dataSource = config[DataSource]("datasource")).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) {
        Interval(stop, start)
      } else {
        Interval(start, stop)
      }
    }

    )
  }


  override def execute(iteration: Int): Unit = {
    logger.warn(s"Iteration $iteration")
    System.gc()
    val g = addGraphUpdateDistribution(graph, config[Int => DistributionType]("distributionType")(iteration))
    val logs = generateLogs(g)

    val time = config[Long]("timestamp")
    val delta = config[Long]("intervalDelta")
    val landyGraph = Landy(logs)
    // logger.warn(s"Number of logs ${logs.count()}") // Warning: calling this line will affect the benchmark
    val timestamp = Interval(time, time + delta)

    val query = landyGraph.directNeighbours(config[Long]("vertexId"), timestamp)

    benchmarks.head.benchmarkAvg(query.collect(), numberOfRuns = 5, customColumnValue = iteration.toString)

    logger.warn(s"This many neighbours ${query.count()}")
  }
}
