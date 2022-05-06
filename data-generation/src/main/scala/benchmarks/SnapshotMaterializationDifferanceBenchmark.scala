package benchmarks

import factories.LogFactory
import thesis.SnapshotIntervalType.Count
import thesis.{SnapshotDelta, VERTEX}

class SnapshotMaterializationDifferanceBenchmark(
                                                  iterationCount: Int = 100,
                                                  customColumn: String = "logs since last materialization"
                                                ) extends QueryBenchmark(iterationCount, customColumn) {
  val numLogs = 100000
  val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numLogs)
  val snapshotGraph = SnapshotDelta(sc.parallelize(logs), Count(10000))

  override def warmUp(): Unit = {
    for (i <- 1 until 10) snapshotGraph.snapshotAtTime(logs.head.timestamp)
  }

  override def execute(iteration: Int): Unit = {
    val materializationOffset = (iteration - 1) * 100
    val timestamp = logs(numLogs / 2 + materializationOffset).timestamp
    benchmarks(0).benchmarkAvg(snapshotGraph.snapshotAtTime(timestamp), numberOfRuns = 3, customColumnValue = materializationOffset.toString)
  }
}
