import factories.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.a
import thesis.Action.{CREATE, DELETE, UPDATE}
import thesis.SnapshotDeltaObject.{createGraph, getSquashedActionsByVertexId, mergeLogTSV}
import thesis.SnapshotIntervalType.{Count, Time}
import thesis.{LogTSV, SnapshotDeltaObject}
import utils.TimeUtils._
import wrappers.SparkTestWrapper

import java.time.Instant
import scala.collection.immutable.HashMap

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
    assertGraphSimilarity(snapshotModel.graphs(0)._1, snapshotModel.graphs(1)._1)
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

    assert(graphs.graphs.head._1.vertices.collect().length == 2)
    assert(graphs.graphs(0)._1.vertices.collect().length == 2)
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
  "mergeLogTSV" should "merge logs correctly given valid states" in {
    val attributes = HashMap(("color", "blue"))
    val newAttributes = HashMap(("color", "red"))

    val update = LogFactory().generateVertexTSV(1, 0, attributes)
    val create = update.copy(action = CREATE)
    val update2 = LogFactory().generateVertexTSV(1, 1, newAttributes)
    val delete = update2.copy(action = DELETE)

    val updateThenDelete = mergeLogTSV(update, delete)
    val createThenDelete = mergeLogTSV(create, delete)

    assert(updateThenDelete.action == DELETE)
    assert(createThenDelete.action == DELETE)

    val updateThenUpdate = mergeLogTSV(update, update2)

    assert(updateThenUpdate.attributes("color") == "red")
    assert(updateThenUpdate.action == UPDATE)

    val createThenUpdate = mergeLogTSV(create, update2)

    assert(createThenUpdate.attributes("color") == "red")
    assert(createThenUpdate.action == CREATE)

  }

  it should "throw exception when invalid state happens" in {
    val update = LogFactory().generateVertexTSV(1, 0)
    val create = update.copy(action = CREATE)
    val delete = update.copy(action = DELETE)
    a[IllegalStateException] should be thrownBy {
      mergeLogTSV(delete, update)
    }
    a[IllegalStateException] should be thrownBy {
      mergeLogTSV(delete, create)
    }
    a[IllegalStateException] should be thrownBy {
      mergeLogTSV(create, create)
    }
    a[IllegalStateException] should be thrownBy {
      mergeLogTSV(delete, delete)
    }
  }

  implicit def seqToRdd(s: Seq[LogTSV])(implicit sc: SparkContext): RDD[LogTSV] = {
    sc.parallelize(s)
  }

  "SnapshotAtTime" should "return correct graph given a timestamp close to a snapshot in the past" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val updates = List(1, 1, 1, 1, 1, 1, 1) // List of amount of updates for t_1, t_2 .. t_6
    val logs = LogFactory().buildIrregularSequence(updates)
    val logRDD = spark.sparkContext.parallelize(logs)
    val snapshotModel = SnapshotDeltaObject.create(logRDD, Time(3))
    val snapshot = snapshotModel.snapshotAtTime(3)
    assertGraphSimilarity(snapshot, createGraph(logs.take(4))) // Take(4) == Instant.ofEpoch(3)
  }

  it should "return correct graph given a timestamp close to a snapshot in the future" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val updates = List(1, 1, 1, 1, 1, 1, 1) // List of amount of updates for t_1, t_2 .. t_6
    val logs = LogFactory().buildIrregularSequence(updates)
    val snapshotModel = SnapshotDeltaObject.create(logs, Time(3))
    val snapshot = snapshotModel.snapshotAtTime(1)
    assertGraphSimilarity(snapshot, createGraph(logs.take(2))) // Take(2) == Instant.ofEpoch(1)
  }

  "returnClosestGraph" should "return the closet graph given an two graphs and an instant" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val vertexLogs1 = LogFactory().buildSingleSequence(5, 1)
    val vertexLogs2 = LogFactory().buildSingleSequence(5, 2)
    val earlyGraph = (createGraph(vertexLogs1), 0: Instant)
    val lateGraph = (createGraph(vertexLogs2), 10: Instant)
    assertGraphSimilarity(lateGraph._1,
      SnapshotDeltaObject.returnClosestGraph(6)(earlyGraph, lateGraph)._1)
    assertGraphSimilarity(earlyGraph._1,
      SnapshotDeltaObject.returnClosestGraph(1)(earlyGraph, lateGraph)._1)
  }
}
