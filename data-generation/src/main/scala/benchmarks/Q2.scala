package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.loadOrGenerateLogs
import thesis.{CorrelationMode, DistributionType, SnapshotDelta, Validity}
import utils.TimeUtils.secondsToInstant

/** Benchmark landy snapshot with a variation of log numbers. */
class Q2(
          distributionType: DistributionType,
          correlationMode: CorrelationMode = CorrelationMode.PositiveCorrelation,
          iterationCount: Int = 5,
          customColumn: String = "Number of logs",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(distributionType, iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    logger.warn(s"i $iteration: Generating distribution and logs")
    val logs = loadOrGenerateLogs(graph, distribution(iteration), dataSource, correlationMode = correlationMode)

    val numberOfLogs = logs.count()
    logger.warn(s"i $iteration: Number of logs $numberOfLogs")

    logger.warn(s"i $iteration: Generating graphs")
    val landyGraph = Validity(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count((numberOfLogs / 10).toInt))

    val timestamp = logs.take((numberOfLogs / 20).toInt).last.timestamp

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.snapshotAtTime(0).graph.persist()
    landyGraph.snapshotAtTime(0).graph.persist()

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg({
      val g = landyGraph.snapshotAtTime(timestamp)
      g.graph.edges.collect()
      g.graph.vertices.collect()
    }, customColumnValue = numberOfLogs.toString)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg({
      val g = snapshotDeltaGraph.snapshotAtTime(timestamp)
      g.graph.edges.collect()
      g.graph.vertices.collect()
    }, customColumnValue = numberOfLogs.toString)

  }
}
