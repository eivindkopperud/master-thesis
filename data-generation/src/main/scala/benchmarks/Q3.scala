package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.loadOrGenerateLogs
import thesis.{DistributionType, Landy, SnapshotDelta, VERTEX}
import utils.TimeUtils.secondsToInstant

class Q3(
          distributionType: DistributionType,
          iterationCount: Int = 5,
          customColumn: String = "number of logs",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(distributionType, iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    logger.warn(s"i $iteration: Generating distribution and logs")
    val logs = loadOrGenerateLogs(graph, distribution(iteration), dataSource)

    val numberOfLogs = logs.count()
    logger.warn(s"i $iteration: Number of logs $numberOfLogs")

    logger.warn(s"i $iteration: Generating graphs")
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count((numberOfLogs / 10).toInt))

    val timestamp = logs.take((numberOfLogs / 20).toInt).last.timestamp

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.getEntity(VERTEX(vertexId), 0L)
    snapshotDeltaGraph.getEntity(VERTEX(vertexId), 0L)

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.getEntity(VERTEX(vertexId), timestamp), customColumnValue = numberOfLogs.toString)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.getEntity(VERTEX(vertexId), timestamp), customColumnValue = numberOfLogs.toString)
  }
}
