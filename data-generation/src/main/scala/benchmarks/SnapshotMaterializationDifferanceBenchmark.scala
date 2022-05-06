package benchmarks

import factories.LogFactory
import thesis.SnapshotIntervalType.Count
import thesis.{SnapshotDelta, VERTEX}

class SnapshotMaterializationDifferanceBenchmark(
                                                  iterationCount: Int = 100,
                                                  customColumn: String = "logs since last materialization"
                                                ) extends QueryBenchmark(iterationCount, customColumn) {
  val numLogs = 10000
  val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numLogs)
  val snapshotGraph = SnapshotDelta(sc.parallelize(logs), Count(1000))

  override def warmUp(): Unit = {
    for (i <- 1 until 100) snapshotGraph.snapshotAtTime(logs.head.timestamp)
  }

  override def execute(iteration: Int): Unit = {
    val materializationOffset = (iteration - 51) * 10
    val timestamp = logs(numLogs / 2 + materializationOffset).timestamp
    System.gc()
    benchmarks(0).benchmarkAvg(snapshotGraph.snapshotAtTime(timestamp), numberOfRuns = 10, customColumnValue = materializationOffset.toString)
  }
}
