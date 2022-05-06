package benchmarks

import factories.LogFactory
import thesis.SnapshotIntervalType.Count
import thesis.{Landy, SnapshotDelta, VERTEX}
import utils.{Benchmark, FileWriter}

/** Benchmark landy snapshot with a variation of log numbers. */
class SnapshotLandyBenchmark(
                              iterationCount: Int = 5,
                              customColumn: String = "number of logs",
                              benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
                            ) extends QueryBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    val numberOfLogs = iteration * 1000
    val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numberOfLogs)
    val landyGraph = Landy(sc.parallelize(logs))
    val snapshotGraph = SnapshotDelta(sc.parallelize(logs), Count(100))
    val timestamp = logs.head.timestamp
    benchmarks(0).benchmarkAvg(landyGraph.snapshotAtTime(timestamp), numberOfRuns = 5, customColumnValue = numberOfLogs.toString)
    benchmarks(1).benchmarkAvg(snapshotGraph.snapshotAtTime(timestamp), numberOfRuns = 5, customColumnValue = numberOfLogs.toString)
  }
}
