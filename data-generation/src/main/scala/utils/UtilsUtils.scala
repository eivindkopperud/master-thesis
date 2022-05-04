package utils

import org.apache.spark.graphx.{Edge, VertexId}
import thesis.DataTypes.{AttributeGraph, Attributes}
import thesis.SnapshotEdgePayload

object UtilsUtils {
  def uuid: Long = java.util.UUID.randomUUID.getLeastSignificantBits & Long.MaxValue

  def zipGraphs(g1: AttributeGraph, g2: AttributeGraph):
  (Seq[((VertexId, Attributes), (VertexId, Attributes))], Seq[(Edge[SnapshotEdgePayload], Edge[SnapshotEdgePayload])]) =
    (g1.vertices.zip(g2.vertices).collect(), g1.edges.zip(g2.edges).collect())
}
