package utils

import org.apache.spark.graphx.{Edge, VertexId}

/** Dictionary
 * With some help from discord this case class was made
 *
 * Use:
 * {{{
 *  val config = Dictionary(urHashMap)
 *  val myVariable = config[YourType]("yourKey")
 *  //instead of
 *  val config = urHashMap
 *  val myVariable = config("yourkey").asInstanceOf[YourType]
 * }}}
 */

case class Dictionary(inner: Map[String, Any]) {
  def apply[V](key: String): V = inner(key).asInstanceOf[V]
}
import thesis.DataTypes.{AttributeGraph, Attributes}
import thesis.SnapshotEdgePayload

object UtilsUtils {
  def uuid: Long = java.util.UUID.randomUUID.getLeastSignificantBits & Long.MaxValue

  def zipGraphs(g1: AttributeGraph, g2: AttributeGraph):
  (Seq[((VertexId, Attributes), (VertexId, Attributes))], Seq[(Edge[SnapshotEdgePayload], Edge[SnapshotEdgePayload])]) =
    (g1.vertices.zip(g2.vertices).collect(), g1.edges.zip(g2.edges).collect())
}
