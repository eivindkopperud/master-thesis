import factories.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.a
import thesis.Action.{CREATE, DELETE, UPDATE}
import thesis.SnapshotDelta.{applyVertexLogsToSnapshot, createGraph, getSquashedActionsByVertexId, mergeLogTSV}
import thesis.SnapshotIntervalType.{Count, Time}
import thesis._
import utils.LogUtils.seqToRdd
import utils.TimeUtils._
import wrappers.SparkTestWrapper

import java.time.Instant
import scala.collection.immutable.HashMap

class SnapshotDeltaSpec extends AnyFlatSpec with SparkTestWrapper {
  "thesis.SnapshotDelta objects" should "have the correct amount of snapshots" in {
    val updateAmount = 5
    val logs = LogFactory().buildSingleSequence(VERTEX(1), updateAmount)
    val logRDD = spark.sparkContext.parallelize(logs)
    val graphs = SnapshotDelta(logRDD, Count(updateAmount - 1))

    assert(graphs.graphs.length == 2)
  }

  def assertGraphSimilarity[VD, ED](g1: Graph[VD, ED], g2: Graph[VD, ED]): Unit = {
    g1.vertices.collect().zip(g2.vertices.collect).foreach {
      case (v1, v2) => assert(v1 == v2)
    }
    g1.edges.collect().zip(g2.edges.collect()).foreach {
      case (e1, e2) => assert(e1 == e2)
    }
  }

  it should "also have the correct amount when it is time based" in {
    val updates = List(1, 1, 0, 0, 1, 1) // List of amount of updates for t_1, t_2 .. t_6
    val logs = LogFactory().buildIrregularVertexSequence(updates)
    val logRDD = spark.sparkContext.parallelize(logs)
    val snapshotModel = SnapshotDelta(logRDD, Time(2))
    assert(snapshotModel.logs.count() == 4)
    assert(snapshotModel.graphs.length == 3)
    assertGraphSimilarity(snapshotModel.graphs(0).graph, snapshotModel.graphs(1).graph)
  }

  it can "consist of only one snapshot" in {
    val updateAmount = 5
    val logs = LogFactory().buildSingleSequence(VERTEX(1), updateAmount)
    val logRDD = spark.sparkContext.parallelize(logs)
    val graphs = SnapshotDelta(logRDD, Count(2 * updateAmount))

    assert(graphs.graphs.length == 1)
  }


  it should "have the right amount of vertices" in {
    val logsVertex1 = LogFactory().buildSingleSequence(VERTEX(1), updateAmount = 5)
    val logsVertex2 = LogFactory().buildSingleSequence(VERTEX(2), updateAmount = 3)
    val logs = spark.sparkContext.parallelize(logsVertex1 ++ logsVertex2)
    val graphs = SnapshotDelta(logs, Count(8))

    assert(graphs.graphs.head.graph.vertices.collect().length == 2)
    assert(graphs.graphs(0).graph.vertices.collect().length == 2)
  }

  "getSquashedActionsByVertexId" should "squash creates correctly" in {
    val vertexLogs = LogFactory().buildSingleSequence(VERTEX(1))
    val logs = spark.sparkContext.parallelize(vertexLogs)
    val edgesWithActions = getSquashedActionsByVertexId(logs).collect()

    assert(edgesWithActions.length == 1)
    assert(edgesWithActions(0)._2.action == CREATE)
  }


  "getSquashedActionsByVertexId" should "squash updates correctly" in {
    val vertexUpdateLogs = LogFactory().buildSingleSequenceOnlyUpdates(VERTEX(1))
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
    val update2 = LogFactory().generateVertexTSV(2, 1)
    val create = update.copy(action = CREATE)
    val delete = update.copy(action = DELETE)
    a[IllegalStateException] should be thrownBy {
      mergeLogTSV(delete, update2)
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
  it should "not throw exception if the timestamps are equal" in {
    val update = LogFactory().generateVertexTSV(1, 0)
    val delete = update.copy(action = DELETE, attributes = HashMap())
    assert(mergeLogTSV(delete, update) == delete)
  }

  "SnapshotAtTime" should "return correct graph given a timestamp close to a snapshot in the past" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val updates = List(1, 1, 1, 1, 1, 1, 1) // List of amount of updates for t_1, t_2 .. t_6
    val logs = LogFactory().buildIrregularVertexSequence(updates)
    val logRDD = spark.sparkContext.parallelize(logs)
    val snapshotModel = SnapshotDelta(logRDD, Time(3))
    val snapshot = snapshotModel.snapshotAtTime(3)
    assertGraphSimilarity(snapshot.graph, createGraph(logs.take(4))) // Take(4) == Instant.ofEpoch(3)
  }

  // backwardsApply is not implemented
  it should "return correct graph given a timestamp close to a snapshot in the future" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val updates = List(1, 1, 1, 1, 1, 1, 1) // List of amount of updates for t_1, t_2 .. t_6
    val logs = LogFactory().buildIrregularVertexSequence(updates)
    val snapshotModel = SnapshotDelta(logs, Time(3))
    val snapshot = snapshotModel.snapshotAtTime(1)
    assertGraphSimilarity(snapshot.graph, createGraph(logs.take(2))) // Take(2) == Instant.ofEpoch(1)
  }

  // This test currently tests an unused function
  "returnClosestGraph" should "return the closet graph given an two graphs and an instant" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val vertexLogs1 = LogFactory().buildSingleSequence(VERTEX(1))
    val vertexLogs2 = LogFactory().buildSingleSequence(VERTEX(2))
    val earlyGraph = Snapshot(createGraph(vertexLogs1), 0: Instant)
    val lateGraph = Snapshot(createGraph(vertexLogs2), 10: Instant)
    assertGraphSimilarity(lateGraph.graph,
      SnapshotDelta.returnClosestGraph(6)(earlyGraph, lateGraph).graph)
    assertGraphSimilarity(earlyGraph.graph,
      SnapshotDelta.returnClosestGraph(1)(earlyGraph, lateGraph).graph)
  }

  "applyVertexLogsToSnapshot" should "apply vertex logs correctly to the given snapshot" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val f = LogFactory()
    val logs = f.buildSingleSequence(VERTEX(1)) ++
      f.buildSingleSequence(EDGE(8, 1, 2)) ++
      f.buildSingleSequence(VERTEX(2))
    val g = createGraph(logs)
    val update = f.buildSingleSequenceOnlyUpdates(VERTEX(1))
    val create = Seq(f.getOne.copy(entity = VERTEX(3), action = CREATE))
    val appliedG = applyVertexLogsToSnapshot(g, update ++ create)
    assert(appliedG.collect().find(x => x._1 == 1).head._2 == update.reverse.head.attributes)
    assert(appliedG.collect().find(x => x._1 == 3).head._2 == create.head.attributes)

  }

  //TODO REFACTOR split this test into two, one for activatedVertices and one for activatedEdges
  "activatedEntities" should "retrieve the activated entities in an interval" in {
    implicit val sparkContext: SparkContext = spark.sparkContext // Needed for implicit conversion of Seq -> RDD

    val vertexLogs1 = LogFactory(startTime = 2, endTime = 8).buildSingleSequence(VERTEX(1))
    val vertexLogs2 = LogFactory(startTime = 5, endTime = 10).buildSingleSequence(VERTEX(2))
    val edgeLogs = LogFactory(startTime = 4, endTime = 7).buildSingleSequence(EDGE(1, 1, 2))
    val logs = (vertexLogs1 ++ vertexLogs2 ++ edgeLogs).sortBy(_.timestamp)
    val g = SnapshotDelta(logs, SnapshotIntervalType.Time(3))
    val intervalWithVertex1 = g.activatedEntities(Interval(0, 3))
    val intervalWithVertex2 = g.activatedEntities(Interval(5, 7))
    val intervalWithEdge = g.activatedEntities(Interval(3, 4))
    val intervalWithV1andEdge = g.activatedEntities(Interval(0, 4))
    val intervalWithV2andEdge = g.activatedEntities(Interval(4, 7))
    val intervalWithAll = g.activatedEntities(Interval(0, 10))


    assert(intervalWithVertex1._1.take(1).head == 1L && intervalWithVertex1._2.isEmpty() && intervalWithVertex1._1.count() == 1)
    assert(intervalWithVertex2._1.take(1).head == 2L && intervalWithVertex2._2.isEmpty() && intervalWithVertex2._1.count() == 1)

    assert(intervalWithEdge._2.take(1).head == 1L && intervalWithEdge._1.isEmpty())

    assert(intervalWithV1andEdge._1.take(1).head == 1L && intervalWithV1andEdge._2.count() == 1)
    assert(intervalWithV2andEdge._1.take(1).head == 2L && intervalWithV2andEdge._2.count() == 1)

    assert(intervalWithAll._1.collect().toSet == Set(1L, 2L) && intervalWithAll._2.count() == 1)
  }

  "Snapshot delta" can "query for direct neighbours in intervals" in {
    implicit val sparkContext: SparkContext = spark.sparkContext // Needed for implicit conversion of Seq -> RDD
    val edgeLogs1 = LogFactory(startTime = t1, endTime = t3).buildSingleSequenceWithDelete(EDGE(100L, 1L, 2L))
    val edgeLogs2 = LogFactory(startTime = t2, endTime = t4).buildSingleSequence(EDGE(200L, 1L, 3L))
    val logs = (edgeLogs1 ++ edgeLogs2).sortBy(_.timestamp)
    val g = SnapshotDelta(logs, SnapshotIntervalType.Count(3))

    // Assertions for 1L's neighbours through time
    assert(g.directNeighbours(1L, Interval(t1, t1)).collect().toSeq == Seq(2L))
    assert(g.directNeighbours(1L, Interval(t2, t2)).collect().toSeq.sorted == Seq(2L, 3L))
    assert(g.directNeighbours(1L, Interval(t2, t3)).collect().toSeq.sorted == Seq(2L, 3L))
    assert(g.directNeighbours(1L, Interval(t4, t4)).collect().toSeq == Seq(3L))

    // Assertions for 2L's neighbours through time
    assert(g.directNeighbours(2L, Interval(t1, t1)).collect().toSeq == Seq(1L))
    assert(g.directNeighbours(2L, Interval(t1, t3)).collect().toSeq == Seq(1L))
    assert(g.directNeighbours(2L, Interval(t4, Instant.MAX)).collect().toSeq == Seq())

    // Assertions for 3L's neighbours through time
    assert(g.directNeighbours(3L, Interval(t1, t1)).collect().toSeq == Seq())
    assert(g.directNeighbours(3L, Interval(t1, t2)).collect().toSeq == Seq(1L))


  }

  "getEntity[Vertex]" should "retrieve the correct vertex at the right timestamp" in {
    implicit val sparkContext: SparkContext = spark.sparkContext

    val v = VERTEX(1)
    val create = LogFactory()
      .getOne
      .copy(timestamp = 0, entity = v, action = CREATE)
    val update = LogFactory().getOne.copy(entity = v, timestamp = 2, action = UPDATE)
    val logs = Seq(create, update)

    val g = SnapshotDelta(logs, SnapshotIntervalType.Time(3))
    // TODO write tests for edges
    val getV = g.getEntity(v, 1)
    val getVAfterUpdate = g.getEntity(v, 3)
    assert(getV.get._2 == create.attributes)
    assert(getVAfterUpdate.get._2 == update.attributes)
  }

  "getEntity[Edge]" should "retrieve the correct edge at the right timestamp" in {
    implicit val sparkContext: SparkContext = spark.sparkContext
    val lf = LogFactory()

    val edge = EDGE(8, 1, 2)
    val eCreate = lf.getOne.copy(entity = edge, action = CREATE, timestamp = 0)
    val eUpdate = lf.getOne.copy(entity = edge, action = UPDATE, timestamp = 2)
    val v1 = LogFactory(startTime = 0L, endTime = 10L).buildSingleSequence(VERTEX(1))
    val v2 = LogFactory(startTime = 0L, endTime = 10L).buildSingleSequence(VERTEX(2))
    val logs = (Seq(eCreate, eUpdate) ++ v1 ++ v2).sortBy(_.timestamp)

    val g = SnapshotDelta(logs, SnapshotIntervalType.Time(3))
    val getEdge = g.getEntity(edge, 1)
    val getEdgeAfterUpdate = g.getEntity(edge, 3)
    assert(getEdge.get._2 == eCreate.attributes)
    assert(getEdgeAfterUpdate.get._2 == eUpdate.attributes)
  }
}
