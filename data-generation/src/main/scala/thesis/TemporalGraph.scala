package thesis

import org.apache.spark.graphx.{EdgeRDD, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.DataTypes.EdgeId

import java.time.Instant
import scala.reflect.ClassTag

abstract class TemporalGraph[VD: ClassTag, ED: ClassTag] extends Serializable {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
  val triplets: RDD[EdgeTriplet[VD, ED]]

  def snapshotAtTime(instant: Instant): Graph[VD, ED]

  /** Return the ids of entities activated or created in the interval
   *
   * @param interval Inclusive interval
   * @return Tuple with the activated entities
   */
  def activatedEntities(interval: Interval): (RDD[VertexId], RDD[EdgeId])

  /**
   * Get all vertices that were in contact with the given vertexId at any time in the interval
   *
   * @param vertexId The id of the vertex we want to find the neighbours of
   * @param interval The time period we want to check for
   * @return The ids of all the neighbours of the queried vertex
   */
  def directNeighbours(vertexId: VertexId, interval: Interval): RDD[VertexId]
}
