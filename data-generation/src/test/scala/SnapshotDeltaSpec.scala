import factories.LogFactory
import org.scalatest.flatspec.AnyFlatSpec
import thesis.Action.{UPDATE, CREATE}
import thesis.SnapshotDeltaObject.getSquashedActionsByVertexId
import thesis.SnapshotDeltaObject
import wrappers.SparkTestWrapper
import thesis.SnapshotIntervalType.Count

class SnapshotDeltaSpec extends AnyFlatSpec with SparkTestWrapper {
  "thesis.SnapshotDelta objects" should "have the correct amount of snapshots" in {
    val updateAmount = 5
    val logs = LogFactory().buildSingleSequence(updateAmount, 1)
    val logRDD = spark.sparkContext.parallelize(logs)
    val graphs = SnapshotDeltaObject.create(logRDD, Count(updateAmount - 1))

    assert(graphs.graphs.length == 2)
  }

  it can "consist of only one snapshot" in {
    val updateAmount = 5
    val logs = LogFactory().buildSingleSequence(updateAmount, 1)
    val logRDD = spark.sparkContext.parallelize(logs)
    val graphs = SnapshotDeltaObject.create(logRDD, Count(2 * updateAmount))

    assert(graphs.graphs.length == 1)
  }

  it should "have the right amount of vertices" in {
    val logsVertex1 = LogFactory().buildSingleSequence(updateAmount = 5, id = 1)
    val logsVertex2 = LogFactory().buildSingleSequence(updateAmount = 3, id = 2)
    val logs = spark.sparkContext.parallelize(logsVertex1 ++ logsVertex2)
    val graphs = SnapshotDeltaObject.create(logs, Count(8))

    assert(graphs.graphs.head.vertices.collect().length == 2)
    assert(graphs.graphs(1).vertices.collect().length == 2)
  }

  "getSquashedActionsByVertexId" should "squash creates correctly" in {
    val vertexLogs = LogFactory().buildSingleSequence(5, 1)
    val logs = spark.sparkContext.parallelize(vertexLogs)
    val edgesWithActions = getSquashedActionsByVertexId(logs).collect()

    assert(edgesWithActions.length == 1)
    assert(edgesWithActions(0)._2.action == CREATE)
  }


  "getSquashedActionsByVertexId" should "squash updates correctly" in {
    val vertexUpdateLogs = LogFactory().buildSingleSequenceOnlyUpdates(5, 1)
    val logs = spark.sparkContext.parallelize(vertexUpdateLogs)
    val edgesWithActions = getSquashedActionsByVertexId(logs).collect()

    assert(edgesWithActions.length == 1)
    assert(edgesWithActions(0)._2.action == UPDATE)
  }
}
