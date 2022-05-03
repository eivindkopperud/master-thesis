package thesis

import org.apache.spark.graphx.{EdgeRDD, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.DataTypes.{Attributes, EdgeId}

import java.time.Instant
import scala.reflect.ClassTag

abstract class TemporalGraph[VD: ClassTag, ED: ClassTag] extends Serializable {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
  val triplets: RDD[EdgeTriplet[VD, ED]]

  def snapshotAtTime(instant: Instant): Graph[VD, ED]


  /**
   * Get all vertices that were in contact with the given vertexId at any time in the interval
   *
   * @param vertexId The id of the vertex we want to find the neighbours of
   * @param interval The time period we want to check for
   * @return The ids of all the neighbours of the queried vertex
   */
  def directNeighbours(vertexId: VertexId, interval: Interval): RDD[VertexId]

  /** Return entity based on ID at a timestamp
   *
   * @param entity    Edge or Vertex
   * @param timestamp Insant
   * @return Entity Object and HashMap
   */
  def getEntity[T <: Entity](entity: T, timestamp: Instant): Option[(T, Attributes)]

  /** Return the ids of the entities that were either
   * activated or created in the interval
   *
   * @param interval Inclusive interval
   * @return Tuple with the activated entities
   */
  final def activatedEntities(interval: Interval): (RDD[VertexId], RDD[EdgeId]) = {
    (activatedVertices(interval), activatedEdges(interval))
  }

  /**
   * Return the ids of vertices that were either
   * activated or created in the given interval
   *
   * @param interval Inclusive interval
   * @return Tuple with the activated entities
   */
  def activatedVertices(interval: Interval): RDD[VertexId]

  /**
   * Return the ids of edges that were either
   * activated or created in the given interval
   *
   * @param interval Inclusive interval
   * @return Tuple with the activated entities
   */
  def activatedEdges(interval: Interval): RDD[EdgeId]
}
