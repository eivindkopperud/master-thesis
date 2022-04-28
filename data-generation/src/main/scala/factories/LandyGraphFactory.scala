package factories

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import thesis.DataTypes.LandyAttributeGraph
import thesis.SparkConfiguration.getSparkSession
import thesis.{LandyEdgePayload, LandyVertexPayload}

import java.time.Instant
import scala.collection.immutable.HashMap

object LandyGraphFactory {
  def createGraph(): LandyAttributeGraph = {
    val spark: SparkSession = getSparkSession

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
            "color" -> "red",
          )
        )
      ),
      (1001L,
        LandyVertexPayload(
          id = 1L,
          validFrom = t2,
          validTo = t3,
          attributes = HashMap(
            "color" -> "orange",
          )
        )
      ),
      (1002L,
        LandyVertexPayload(
          id = 2L,
          validFrom = t1,
          validTo = t3,
          attributes = HashMap(
            "color" -> "red",
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
            "relation" -> "brother"
          )
        )
      ),
      Edge(1L, 2L,
        LandyEdgePayload(
          id = 1004L,
          validFrom = t2,
          validTo = t3,
          attributes = HashMap(
            "relation" -> "brother"
          )
        )
      )
    )
  }
}
