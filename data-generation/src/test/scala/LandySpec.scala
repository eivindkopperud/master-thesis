import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import thesis.{Landy, LandyEdgePayload, LandyVertexPayload}
import org.scalatest.flatspec.AnyFlatSpec
import thesis.LTSV.Attributes
import thesis.SnapshotDeltaObject.LandyAttributeGraph
import wrappers.SparkTestWrapper

import java.time.Instant
import scala.collection.immutable.HashMap

class LandySpec extends AnyFlatSpec with SparkTestWrapper {

  def createGraph(): LandyAttributeGraph = {
    val vertices = spark.sparkContext.parallelize(getVertices)
    val edges = spark.sparkContext.parallelize(getEdges)

    Graph(vertices, edges)
  }
  val t1 = Instant.parse("2000-01-01T00:00:00.000Z")
  val t2 = Instant.parse("2001-01-01T00:00:00.000Z")
  val t3 = Instant.parse("2002-01-01T00:00:00.000Z")
  def getVertices(): Seq[(Long, LandyVertexPayload)] = {

    Seq(
      (1000L,
        LandyVertexPayload(
          id = 1L,
          validFrom = t1,
          validTo = t2,
          attributes = HashMap(
            "color"->"red",
          )
        )
      ),
      (1001L,
        LandyVertexPayload(
          id = 1L,
          validFrom = t2,
          validTo = t3,
          attributes = HashMap(
            "color"->"orange",
          )
        )
      ),
      (1002L,
        LandyVertexPayload(
          id = 2L,
          validFrom = t1,
          validTo = t3,
          attributes = HashMap(
            "color"->"red",
          )
        )
      )
    )
  }

  def getEdges(): Seq[Edge[LandyEdgePayload]] = {
    Seq(
      Edge(1L, 2L,
        LandyEdgePayload(
          id = 1003L,
          validFrom = t1,
          validTo = t2,
          attributes = HashMap(
            "relation"->"brother"
          )
        )
      ),
      Edge(1L, 2L,
        LandyEdgePayload(
          id = 1004L,
          validFrom = t2,
          validTo = t3,
          attributes = HashMap(
            "relation"->"brother"
          )
        )
      )
    )
  }

  "Landy" can "be created" in {
    val landy: Landy = new Landy(createGraph())

    assert(landy.vertices.count() == 5)
    assert(landy.edges.count() == 2)
  }

  it can "give a snapshot" in {
    val landy: Landy = new Landy(createGraph())

    val graph = landy.snapshotAtTime(t2)

    assert(graph.vertices.count() == 2)
    assert(graph.edges.count() == 1)
  }
}