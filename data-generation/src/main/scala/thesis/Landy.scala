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
    val vertices = this.vertices.filter(vertex => (vertex._2.get("timeFrom"), vertex._2.get("timeTo")) match {
      case (Some(timeFrom), Some(timeTo)) => Instant.parse(timeFrom).isBefore(instant) && Instant.parse(timeTo).isBefore(instant)
      case _ => throw new IllegalStateException("All Landy objects should have timeFrom and timeTo as attributes.")
    })

    val edges = this.edges.filter(edge => (edge.attr.get("timeFrom"), edge.attr.get("timeTo")) match {
      case (Some(timeFrom), Some(timeTo)) => Instant.parse(timeFrom).isBefore(instant) && Instant.parse(timeTo).isBefore(instant)
      case _ => throw new IllegalStateException("All Landy objects should have timeFrom and timeTo as attributes.")
    })

    Graph(vertices, edges)
  }
}
