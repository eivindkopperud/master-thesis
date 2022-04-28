package thesis

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import thesis.DataTypes.{EdgeId, LandyAttributeGraph}

import java.time.Instant
import scala.math.Ordered.orderingToOrdered


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

  /** Return the ids of entities activated or created in the interval
   *
   * @param interval Inclusive interval
   * @return Tuple with the activated entities
   */
  override def activatedEntities(interval: Interval): (RDD[VertexId], RDD[EdgeId]) = throw new NotImplementedError()
}
