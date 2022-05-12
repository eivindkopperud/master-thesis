package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Landy, SnapshotDelta, VERTEX}
import utils.TimeUtils.secondsToInstant

class Q3(
          iterationCount: Int = 5,
          customColumn: String = "number of logs",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    logger.warn(s"i $iteration: Generating distribution")
    val g = addGraphUpdateDistribution(graph, distribution(iteration))
    logger.warn(s"i $iteration: Generating logs")
    val logs = generateLogs(g)

    val numberOfLogs = logs.count().toString
    logger.warn(s"i $iteration: Number of logs $numberOfLogs")

    logger.warn(s"i $iteration: Generating graphs")
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count(intervalDelta))

    val expectedLogPrEntity = (iteration + 1).toString

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.getEntity(VERTEX(vertexId), 0L)
    snapshotDeltaGraph.getEntity(VERTEX(vertexId), 0L)

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.getEntity(VERTEX(vertexId), timestamp), customColumnValue = numberOfLogs)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.getEntity(VERTEX(vertexId), timestamp), customColumnValue = numberOfLogs)
  }
}
