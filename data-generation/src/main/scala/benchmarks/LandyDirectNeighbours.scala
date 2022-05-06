package benchmarks

import thesis.DataSource.FbMessages
import thesis.DistributionType.UniformType
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Interval, Landy}
import utils.TimeUtils.secondsToInstant


class LandyDirectNeighbours(iterationCount: Int = 3,
                            customColumn: String = "Landy direct neighbours"
                           ) extends QueryBenchmark(iterationCount, customColumn) {

  lazy val graph = generateGraph(20, dataSource = FbMessages)


  override def execute(iteration: Int): Unit = {
    println(s"Iteration $iteration")
    val g = addGraphUpdateDistribution(graph, UniformType(1, 2 * (iteration + 1)))
    val logs = generateLogs(g)

    val time = 1082148639L
    val landyGraph = Landy(logs)
    val timestamp = Interval(time, time + 1000)
    benchmarks.head.benchmarkAvg(landyGraph.directNeighbours(1356, timestamp), numberOfRuns = 5, customColumnValue = iteration.toString)
  }
}
