package factories

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import thesis.DataTypes.LandyAttributeGraph
import thesis.SparkConfiguration.getSparkSession
import thesis.LandyEntityPayload
import utils.TimeUtils.{t1, t2, t3}

import java.time.Instant
import scala.collection.immutable.HashMap

object LandyGraphFactory {
  def createGraph(): LandyAttributeGraph = {
    val spark: SparkSession = getSparkSession

    val vertices = spark.sparkContext.parallelize(getVertices)
    val edges = spark.sparkContext.parallelize(getEdges)

    Graph(vertices, edges)
  }

  def getVertices(): Seq[(VertexId, LandyEntityPayload)] = {
    Seq(
      (1000L,
        LandyEntityPayload(
          id = 1L,
          validFrom = t1,
          validTo = t2,
          attributes = HashMap(
            "color" -> "red",
          )
        )
      ),
      (1001L,
        LandyEntityPayload(
          id = 1L,
          validFrom = t2,
          validTo = t3,
          attributes = HashMap(
            "color" -> "orange",
          )
        )
      ),
      (1002L,
        LandyEntityPayload(
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

  def getEdges(): Seq[Edge[LandyEntityPayload]] = {
    Seq(
      Edge(1L, 2L,
        LandyEntityPayload(
          id = 1003L,
          validFrom = t1,
          validTo = t2,
          attributes = HashMap(
            "relation" -> "brother"
          )
        )
      ),
      Edge(1L, 2L,
        LandyEntityPayload(
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
