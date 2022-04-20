import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import thesis.Landy
import org.scalatest.flatspec.AnyFlatSpec
import thesis.LTSV.Attributes
import thesis.SnapshotDeltaObject.G
import wrappers.SparkTestWrapper

import java.time.Instant
import scala.collection.immutable.HashMap

class LandySpec extends AnyFlatSpec with SparkTestWrapper {

  def createGraph(): G = {
    val vertices = spark.sparkContext.parallelize(getVertices)
    val edges = spark.sparkContext.parallelize(getEdges)
    Graph(vertices, edges)
  }
  val t1 = "2000-01-01T00:00:00.000Z"
  val t2 = "2001-01-01T00:00:00.000Z"
  val t3 = "2002-01-01T00:00:00.000Z"
  def getVertices(): Seq[(Long, Attributes)] = {

    Seq(
      (1L, HashMap(
        "validFrom"->t1,
        "validTo"->t2,
        "color"->"red")
      ),
      (2L, HashMap(
        "validFrom"->t2,
        "validTo"->t3,
        "color"->"blue"
      )),
      (3L, HashMap(
        "validFrom"->t1,
        "validTo"->t3,
        "color"->"orange"
      ))
    )
  }

  def getEdges(): Seq[Edge[Attributes]] = {
    Seq(
      Edge(1L, 3L, HashMap(
        "validFrom"->t1,
        "validTo"->t2,
        "relation"->"brother")
    ),
      Edge(2L, 3L, HashMap(
        "validFrom"->t2,
        "validTo"->t3,
        "color"->"sister")
      )
    )
  }

  "Landy" can "be created" in {
    val landy: Landy = new Landy(createGraph())

    assert(landy.vertices.count() == 3)
    assert(landy.edges.count() == 2)
  }

  it can "give a snapshot" in {
    val landy: Landy = new Landy(createGraph())
    val instant = Instant.parse(t2)

    val graph = landy.snapshotAtTime(instant)

    assert(graph.vertices.count() == 2)
    assert(graph.edges.count() == 1)
  }
}