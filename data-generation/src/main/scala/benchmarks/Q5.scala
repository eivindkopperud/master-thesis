package benchmarks

import factories.LogFactory
import thesis.SnapshotIntervalType.Count
import thesis.{Snapshot, SnapshotDelta, VERTEX}

import java.time.Instant

class Q5(
          iterationCount: Int = 20,
          customColumn: String = "logs since last materialization"
        ) extends QueryBenchmark(iterationCount, customColumn) {
  val numLogs = 100000
  val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numLogs)
  val snapshotGraph: SnapshotDelta = SnapshotDelta(sc.parallelize(logs), Count(1000))

  override def execute(iteration: Int): Unit = {
    val materializationOffset = (iteration - 2) * 50
    val timestamp = logs(numLogs / 2 - 1 + materializationOffset).timestamp
    sc.getPersistentRDDs.foreach(_._2.unpersist())
    benchmarks(0).benchmarkAvg(doQuery(timestamp), numberOfRuns = 10, customColumnValue = materializationOffset.toString)
  }

  def doQuery(timestamp: Instant): Unit = {
    val Snapshot(graph, _) = snapshotGraph.snapshotAtTime(timestamp)
    graph.vertices.collect()
    graph.edges.collect()
  }
}
