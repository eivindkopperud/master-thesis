package thesis

import org.apache.spark.graphx.{EdgeRDD, EdgeTriplet, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.LTSV.Attributes
import thesis.SnapshotDeltaObject.G

import java.time.Instant


class Landy(val graph: G) extends TemporalGraph[Attributes, Attributes] {
  override val vertices: VertexRDD[Attributes] = graph.vertices
  override val edges: EdgeRDD[Attributes] = graph.edges
  override val triplets: RDD[EdgeTriplet[Attributes, Attributes]] = graph.triplets

  override def snapshotAtTime(instant: Instant): Graph[Attributes, Attributes] = {
    val vertices = this.vertices.filter(vertex => (vertex._2.get("validFrom"), vertex._2.get("validTo")) match {
      case (Some(validFrom), Some(validTo)) => Instant.parse(validFrom).isBefore(instant) && (Instant.parse(validTo).isBefore(instant) || Instant.parse(validTo).equals(instant))
      case _ => throw new IllegalStateException("All Landy objects should have validFrom and validTo as attributes.")
    })

    val edges = this.edges.filter(edge => (edge.attr.get("validFrom"), edge.attr.get("validTo")) match {
      case (Some(validFrom), Some(validTo)) => Instant.parse(validFrom).isBefore(instant) && (Instant.parse(validTo).isBefore(instant) || Instant.parse(validTo).equals(instant))
      case _ => throw new IllegalStateException("All Landy objects should have validFrom and validTo as attributes.")
    })

    Graph(vertices, edges)
  }
}


