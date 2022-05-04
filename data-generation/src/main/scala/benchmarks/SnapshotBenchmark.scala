package benchmarks

import factories.LogFactory
import thesis.{Landy, VERTEX}

/** Benchmark landy snapshot with a variation of log numbers. */
class SnapshotLandyBenchmark(iterationCount: Int = 5, customColumn: String = "number of logs") extends QueryBenchmark(iterationCount, customColumn) {
  override def execute(iteration: Int): Unit = {
    val numberOfLogs = iteration * 1000
    val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numberOfLogs)
    val graph = Landy(sc.parallelize(logs))
    val timestamp = logs.head.timestamp
    benchmark.benchmarkAvg(graph.snapshotAtTime(timestamp), numberOfRuns = 5, customColumnValue = numberOfLogs.toString)
  }
}
