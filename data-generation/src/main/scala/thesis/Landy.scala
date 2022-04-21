package thesis

import org.apache.spark.graphx.{EdgeRDD, EdgeTriplet, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.LTSV.Attributes
import thesis.SnapshotDeltaObject.AttributeGraph

import java.time.Instant

final case class InvalidTimeSchemaError(
                                         private val message: String = "All Landy objects should have validFrom and validTo as attributes.",
                                         private val cause: Throwable = None.orNull
                                       ) extends IllegalStateException(message, cause)


class Landy(val graph: AttributeGraph) extends TemporalGraph[Attributes, Attributes] {
  override val vertices: VertexRDD[Attributes] = graph.vertices
  override val edges: EdgeRDD[Attributes] = graph.edges
  override val triplets: RDD[EdgeTriplet[Attributes, Attributes]] = graph.triplets

  override def snapshotAtTime(instant: Instant): Graph[Attributes, Attributes] = {
    val vertices = this.vertices.filter(vertex => (vertex._2.get("validFrom"), vertex._2.get("validTo")) match {
      case (Some(validFrom), Some(validTo)) => Instant.parse(validFrom).isBefore(instant) && (Instant.parse(validTo).isBefore(instant) || Instant.parse(validTo).equals(instant))
      case _ => throw new InvalidTimeSchemaError
    }).map(vertex => vertex._2.get("vertexId") match {
      case Some(value) => (value.toLong, vertex._2)
      case None => throw new IllegalStateException("Vertices must have the original id stuff")
    })

    val edges = this.edges.filter(edge => (edge.attr.get("validFrom"), edge.attr.get("validTo")) match {
      case (Some(validFrom), Some(validTo)) => Instant.parse(validFrom).isBefore(instant) && (Instant.parse(validTo).isBefore(instant) || Instant.parse(validTo).equals(instant))
      case _ => throw new InvalidTimeSchemaError
    })

    Graph(vertices, edges)
  }
}


