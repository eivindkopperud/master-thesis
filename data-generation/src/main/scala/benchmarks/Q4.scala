package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.loadOrGenerateLogs
import thesis.{Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

import java.time.temporal.ChronoUnit

class Q4(
          iterationCount: Int = 5,
          customColumn: String = "Number of logs",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    logger.warn(s"i $iteration: Generating distribution and logs")
    val logs = loadOrGenerateLogs(graph, distribution(iteration))

    val numberOfLogs = logs.count().toString
    logger.warn(s"i $iteration: Number of logs $numberOfLogs")

    logger.warn(s"i $iteration: Generating graphs")
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count(intervalDelta))

    val expectedLogPrEntity = (iteration + 1).toString
    val min = logs.map(_.timestamp).takeOrdered(1).head
    val max = logs.map(_.timestamp).top(1).head
    val diff = min.until(max, ChronoUnit.SECONDS)
    val interval = Interval(min.getEpochSecond + (diff * 0.25).toLong, max.getEpochSecond - (diff * 0.25).toLong)
    logger.warn(s"i $iteration $interval")

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.activatedEntities(Interval(0, 0))
    snapshotDeltaGraph.activatedEntities(Interval(0, 0))

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg({
      val (vertexIds, edgeIds) = landyGraph.activatedEntities(interval)
      vertexIds.collect()
      edgeIds.collect()
    }, customColumnValue = numberOfLogs)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg({
      val (vertexIds, edgeIds) = snapshotDeltaGraph.activatedEntities(interval)
      vertexIds.collect()
      edgeIds.collect()
    }, customColumnValue = numberOfLogs)
  }
}
