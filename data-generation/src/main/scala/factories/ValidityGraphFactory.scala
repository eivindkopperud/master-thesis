package factories

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import thesis.DataTypes.ValidityAttributeGraph
import thesis.SparkConfiguration.getSparkSession
import thesis.ValidityEntityPayload
import utils.TimeUtils.{t1, t2, t3, t4}

import scala.collection.immutable.HashMap

object ValidityGraphFactory {
  def createGraph(): ValidityAttributeGraph = {
    val spark: SparkSession = getSparkSession

    val vertices = spark.sparkContext.parallelize(getVertices)
    val edges = spark.sparkContext.parallelize(getEdges)

    Graph(vertices, edges)
  }

  def getVertices(): Seq[(VertexId, ValidityEntityPayload)] = {
    Seq(
      (1000L,
        ValidityEntityPayload(
          id = 1L,
          validFrom = t1,
          validTo = t2,
          attributes = HashMap(
            "color" -> "red",
          )
        )
      ),
      (1001L,
        ValidityEntityPayload(
          id = 1L,
          validFrom = t2,
          validTo = t3,
          attributes = HashMap(
            "color" -> "orange",
          )
        )
      ),
      (1002L,
        ValidityEntityPayload(
          id = 2L,
          validFrom = t1,
          validTo = t4,
          attributes = HashMap(
            "color" -> "red",
          )
        )
      )
    )
  }

  def getEdges(): Seq[Edge[ValidityEntityPayload]] = {
    Seq(
      Edge(1L, 2L,
        ValidityEntityPayload(
          id = 1003L,
          validFrom = t1,
          validTo = t2,
          attributes = HashMap(
            "relation" -> "brother"
          )
        )
      ),
      Edge(1L, 2L,
        ValidityEntityPayload(
          id = 1003L,
          validFrom = t2,
          validTo = t3,
          attributes = HashMap(
            "relation" -> "sister"
          )
        )
      )
    )
  }
}
