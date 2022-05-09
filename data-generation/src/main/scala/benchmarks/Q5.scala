package benchmarks

import factories.LogFactory
import thesis.SnapshotIntervalType.Count
import thesis.{Snapshot, SnapshotDelta, VERTEX}

import java.time.Instant

class Q5(
          iterationCount: Int = 10,
          customColumn: String = "logs since last materialization"
        ) extends QueryBenchmark(iterationCount, customColumn) {
  val numLogs = 100
  val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numLogs)
  val snapshotGraph: SnapshotDelta = SnapshotDelta(sc.parallelize(logs), Count(10))

  override def execute(iteration: Int): Unit = {
    val materializationOffset = (iteration - 2) % 10
    val timestamp = logs(numLogs / 2 + materializationOffset).timestamp
    sc.getPersistentRDDs.foreach(_._2.unpersist())
    benchmarks(0).benchmarkAvg(doQuery(timestamp), numberOfRuns = 5, customColumnValue = materializationOffset.toString)
  }

  def doQuery(timestamp: Instant): Unit = {
    val Snapshot(graph, _) = snapshotGraph.snapshotAtTime(timestamp)
    graph.vertices.collect()
    graph.edges.collect()
  }
}
