package thesis

import org.apache.spark.graphx.{Edge, EdgeRDD, EdgeTriplet, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.LTSV.Attributes
import thesis.SnapshotDeltaObject.LandyAttributeGraph

import java.time.Instant
import scala.math.Ordered.orderingToOrdered

case class LandyVertexPayload(id: Long, validFrom: Instant, validTo:Instant, attributes:Attributes)
case class LandyEdgePayload(id: Long, validFrom: Instant, validTo:Instant, attributes:Attributes)


class Landy(val graph: LandyAttributeGraph) extends TemporalGraph[LandyVertexPayload, LandyEdgePayload] {

  override val vertices: VertexRDD[LandyVertexPayload] = graph.vertices
  override val edges: EdgeRDD[LandyEdgePayload] = graph.edges
  override val triplets: RDD[EdgeTriplet[LandyVertexPayload, LandyEdgePayload]] = graph.triplets

  override def snapshotAtTime(instant: Instant): Graph[LandyVertexPayload, LandyEdgePayload] = {
    val notNullVertices = this.vertices.filter(vertex => vertex._2 != null) // Required since the vertex payload is null when "floating edges" are created
    val vertices = notNullVertices.filter(vertex =>
      vertex._2.validFrom < instant &&
      vertex._2.validTo >= instant
    ).map(vertex => (vertex._2.id, vertex._2))

    val edges = this.edges.filter(edge =>
        edge.attr.validFrom < instant &&
        edge.attr.validTo >= instant
      )
    Graph(vertices, edges)
  }
}


