package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.loadOrGenerateLogs
import thesis.{DistributionType, Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant
import utils.UtilsUtils.CollectTuple

class Q4(
          distributionType: DistributionType,
          iterationCount: Int = 5,
          customColumn: String = "Number of logs",
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

    val min = logs.map(_.timestamp).takeOrdered(1).head
    val max = logs.map(_.timestamp).top(1).head
    val interval = Interval(min, (max.getEpochSecond / 2))
    logger.warn(s"i $iteration $interval")

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.activatedEntities(Interval(0, 0)).collect()
    snapshotDeltaGraph.activatedEntities(Interval(0, 0)).collect()

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.activatedEntities(interval).collect(), customColumnValue = numberOfLogs.toString)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.activatedEntities(interval).collect(), customColumnValue = numberOfLogs.toString)
  }
}
