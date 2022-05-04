import factories.LogFactory
import org.apache.spark.SparkContext
import org.scalatest.flatspec.AnyFlatSpec
import thesis.SnapshotIntervalType.Time
import thesis.{Landy, SnapshotDelta}
import utils.LogUtils.seqToRdd
import utils.TimeUtils.secondsToInstant
import wrappers.SparkTestWrapper

class SnapshotLandySpec extends AnyFlatSpec with SparkTestWrapper {

  "getEntity" should "be equal for Landy and SnapshotDelta" in {
    implicit val sparkContext: SparkContext = spark.sparkContext // Needed for implicit conversion of Seq -> RDD

    val lf = LogFactory(startTime = 0L, endTime = 1000L)
    val entities = lf
      .getRandomEntities
    print(entities)
    val logs = entities
      .flatMap(lf.buildSingleSequenceWithDelete(_))
      .sortBy(_.timestamp)
    logs.foreach(println)
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Time(100))
    snapshotDeltaGraph.graphs.foreach(g => println(g.instant))
    val instant = 45L
    entities.foreach(ent => {
      assert(landyGraph.getEntity(ent, instant) == snapshotDeltaGraph.getEntity(ent, instant))
    })


  }

}
