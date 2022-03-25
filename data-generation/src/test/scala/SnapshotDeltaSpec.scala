import factories.LogFactory
import org.apache.spark.graphx.Graph
import org.scalatest.flatspec.AnyFlatSpec
import thesis.Action.{CREATE, UPDATE}
import thesis.SnapshotDeltaObject
import thesis.SnapshotDeltaObject.getSquashedActionsByVertexId
import thesis.SnapshotIntervalType.{Count, Time}
import wrappers.SparkTestWrapper

class SnapshotDeltaSpec extends AnyFlatSpec with SparkTestWrapper {
  "thesis.SnapshotDelta objects" should "have the correct amount of snapshots" in {
    val updateAmount = 5
    val logs = LogFactory().buildSingleSequence(updateAmount, 1)
    val logRDD = spark.sparkContext.parallelize(logs)
    val graphs = SnapshotDeltaObject.create(logRDD, Count(updateAmount - 1))

    assert(graphs.graphs.length == 2)
  }

  // Can be run if "ignore" is swapped with "it"
  it should "also have the correct amount when it is time based" in {
    val updates = List(1, 1, 0, 0, 1, 1) // List of amount of updates for t_1, t_2 .. t_6
    val logs = LogFactory().buildIrregularSequence(updates)
    val logRDD = spark.sparkContext.parallelize(logs)
    val snapshotModel = SnapshotDeltaObject.create(logRDD, Time(2))
    assert(snapshotModel.logs.count() == 4)
    assert(snapshotModel.graphs.length == 3)
    assertGraphSimilarity(snapshotModel.graphs(0), snapshotModel.graphs(1))
  }

  it can "consist of only one snapshot" in {
    val updateAmount = 5
    val logs = LogFactory().buildSingleSequence(updateAmount, 1)
    val logRDD = spark.sparkContext.parallelize(logs)
    val graphs = SnapshotDeltaObject.create(logRDD, Count(2 * updateAmount))

    assert(graphs.graphs.length == 1)
  }

  def assertGraphSimilarity[VD, ED](g1: Graph[VD, ED], g2: Graph[VD, ED]): Unit = {
    g1.vertices.collect().zip(g2.vertices.collect).foreach {
      case (v1, v2) => assert(v1 == v2)
    }
    g1.edges.collect().zip(g2.edges.collect()).foreach {
      case (e1, e2) => assert(e1 == e2)
    }
  }

  it should "have the right amount of vertices" in {
    val logsVertex1 = LogFactory().buildSingleSequence(updateAmount = 5, id = 1)
    val logsVertex2 = LogFactory().buildSingleSequence(updateAmount = 3, id = 2)
    val logs = spark.sparkContext.parallelize(logsVertex1 ++ logsVertex2)
    val graphs = SnapshotDeltaObject.create(logs, Count(8))

    assert(graphs.graphs.head.vertices.collect().length == 2)
    assert(graphs.graphs(0).vertices.collect().length == 2)
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
