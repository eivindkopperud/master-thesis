import factories.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec
import thesis.SnapshotIntervalType.Time
import thesis.{EDGE, Entity, Interval, Landy, LogTSV, SnapshotDelta, VERTEX}
import utils.LogUtils.seqToRdd
import utils.TimeUtils.secondsToInstant
import wrappers.SparkTestWrapper

class SnapshotLandySpec extends FixtureAnyFlatSpec with SparkTestWrapper {

  case class FixtureParam(factory: LogFactory,
                          entities: Seq[Entity],
                          landyGraph: Landy,
                          snapshotDelta: SnapshotDelta,
                          logs: Seq[LogTSV])

  override def withFixture(test: OneArgTest): Outcome = {
    implicit val sparkContext: SparkContext = spark.sparkContext // Needed for implicit conversion of Seq -> RDD
    val factory = LogFactory(startTime = 10L, endTime = 1000L)
    val entities = factory
      .getRandomEntities(0, 10)
    val logs1 = entities
      .flatMap(factory.buildSingleSequenceWithDelete(_))

    val offsetFactory = LogFactory(startTime = 250L, endTime = 750L)
    val offsetEntities = offsetFactory
      .getRandomEntities(11, 20)
    val logs2 = offsetEntities
      .flatMap(offsetFactory.buildSingleSequenceWithDelete(_))
    val logs = (logs1 ++ logs2).sortBy(_.timestamp)
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Time(100))
    withFixture(test.toNoArgTest(FixtureParam(
      factory, entities, landyGraph, snapshotDeltaGraph, logs
    )))
  }

  "getEntity" should "be equal for Landy and SnapshotDelta" in { f =>
    val FixtureParam(_, entities, landyGraph, snapshotDeltaGraph, _) = f

    val instant = 45L
    entities.foreach(ent => {
      assert(landyGraph.getEntity(ent, instant) == snapshotDeltaGraph.getEntity(ent, instant))
    })
  }

  def assertGraphSimilarity[VD, ED](g1: Graph[VD, ED], g2: Graph[VD, ED]): Unit = {
    g1.vertices.collect().zip(g2.vertices.collect).foreach {
      case (v1, v2) => assert(v1 == v2)
    }
    g1.edges.collect().zip(g2.edges.collect()).foreach {
      case (e1, e2) => assert(e1 == e2)
    }
  }

  "snapshotAtTime" should "be equal for Landy and SnapshotDelta" in { f =>
    val FixtureParam(_, _, landyGraph, snapshotDeltaGraph, _) = f
    val instants = Seq(21, 53, 77)
    instants.foreach(t => {
      val landyGraphT = landyGraph.snapshotAtTime(t).graph
      val snapshotDeltaGraphT = snapshotDeltaGraph.snapshotAtTime(t).graph
      assertGraphSimilarity(landyGraphT, snapshotDeltaGraphT)
    })
  }


  "activatedEntities" should "be equal for Landy and SnapshotDelta" in { f =>
    val FixtureParam(_, _, landyGraph, snapshotDeltaGraph, _) = f
    val onlyFirstBatch = Interval(0, 100)
    val bothBatches = Interval(0, 300)
    val onlyLastBatch = Interval(210, 300)
    val intervals = Seq(onlyFirstBatch, bothBatches, onlyLastBatch)
    intervals.foreach(interval => {
      val (landyVertices, landyEdges) = landyGraph.activatedEntities(interval)
      val (deltaVertices, deltaEdges) = snapshotDeltaGraph.activatedEntities(interval)

      landyVertices.collect().sorted.zip(deltaVertices.collect().sorted).foreach({ case (v1, v2) => assert(v1 == v2) })
      landyEdges.collect().sorted.zip(deltaEdges.collect().sorted).foreach({ case (e1, e2) => assert(e1 == e2) })
    })
  }

  "directNeighbours" should "be equal for Landy and SnapshotDelta" in { f =>
    val FixtureParam(_, entities, landyGraph, snapshotDeltaGraph, _) = f
    val vertices = entities.flatMap(e => e match {
      case v: VERTEX => Some(v)
      case _: EDGE => None
    })
    val bothBatches = Interval(0, 300)
    vertices.foreach(v => {
      val lVertexIds = landyGraph.directNeighbours(v.id, bothBatches).collect().sorted
      val sdVertexIds = snapshotDeltaGraph.directNeighbours(v.id, bothBatches).collect().sorted
      println(lVertexIds.mkString("Array(", ", ", ")"))
      println(sdVertexIds.mkString("Array(", ", ", ")"))
      lVertexIds.zip(sdVertexIds).foreach({ case (vId, uId) => assert(vId == uId) })
    }
    )


  }
}
