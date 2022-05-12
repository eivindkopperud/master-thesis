package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

import java.time.Instant

class Q1(
          iterationCount: Int = 5,
          customColumn: String = "average number of logs for each entity",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    logger.warn(s"i $iteration: Generating distribution")
    val g = addGraphUpdateDistribution(graph, distribution(iteration))
    logger.warn(s"i $iteration: Generating logs")
    val logs = generateLogs(g)

    logger.warn(s"i $iteration: Number of logs ${logs.count()}")

    logger.warn(s"i $iteration: Generating graphs")
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count(intervalDelta))

    val interval = Interval(0, Instant.MAX)

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.directNeighbours(vertexId, Interval(0, 0)).collect()
    snapshotDeltaGraph.directNeighbours(vertexId, Interval(0, 0)).collect()

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.directNeighbours(vertexId, interval).collect(), customColumnValue = getMean(iteration).toString)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.directNeighbours(vertexId, interval).collect(), customColumnValue = getMean(iteration).toString)
  }
}
