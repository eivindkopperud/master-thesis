package benchmarks

import factories.LogFactory
import thesis.{SnapshotDelta, SnapshotIntervalType, VERTEX}


class CountBasedSnapshotBenchmark(iterationCount: Int = 5, customColumn: String = "logs between each snapshot") extends QueryBenchmark(iterationCount, customColumn) {
  override def execute(iteration: Int): Unit = {
    val logs = LogFactory().buildSingleSequence(VERTEX(1), 10000)
    val logsBetweenEachMaterializedSnapshot = iteration * 100
    val snapshotDelta = SnapshotDelta(sc.parallelize(logs), SnapshotIntervalType.Count(logsBetweenEachMaterializedSnapshot))

    benchmarks(0).benchmarkAvg(snapshotDelta.getEntity(VERTEX(1), logs(0).timestamp), customColumnValue = logsBetweenEachMaterializedSnapshot.toString)
  }
}
