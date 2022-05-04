import factories.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec
import thesis.SnapshotIntervalType.Time
import thesis.{Entity, Landy, LogTSV, SnapshotDelta}
import utils.LogUtils.seqToRdd
import utils.TimeUtils.secondsToInstant
import wrappers.SparkTestWrapper

class SnapshotLandySpec extends FixtureAnyFlatSpec with SparkTestWrapper {

  case class FixtureParam(lf: LogFactory,
                          entities: Seq[Entity],
                          landyGraph: Landy,
                          snapshotDelta: SnapshotDelta,
                          logs: Seq[LogTSV])

  override def withFixture(test: OneArgTest): Outcome = {
    implicit val sparkContext: SparkContext = spark.sparkContext // Needed for implicit conversion of Seq -> RDD
    val lf = LogFactory(startTime = 0L, endTime = 1000L)
    val entities = lf
      .getRandomEntities
    val logs = entities
      .flatMap(lf.buildSingleSequenceWithDelete(_))
      .sortBy(_.timestamp)
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Time(100))
    withFixture(test.toNoArgTest(FixtureParam(
      lf, entities, landyGraph, snapshotDeltaGraph, logs
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
}
