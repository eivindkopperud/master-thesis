package thesis

import org.apache.spark.graphx.{Edge, EdgeContext, EdgeRDD, EdgeTriplet, Graph, GraphOps, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.DataTypes.{AttributeGraph, Attributes, EdgeId}

import java.time.Instant
import scala.reflect.ClassTag

abstract class TemporalGraph extends Serializable {
  lazy val vertices: VertexRDD[Attributes] = graph.vertices
  lazy val edges: EdgeRDD[SnapshotEdgePayload] = graph.edges
  lazy val triplets: RDD[EdgeTriplet[Attributes, SnapshotEdgePayload]] = graph.triplets

  lazy val graph: AttributeGraph = snapshotAtTime(Instant.now()).graph
  lazy val ops: GraphOps[Attributes, SnapshotEdgePayload] = graph.ops

  def getCurrentGraph: AttributeGraph = snapshotAtTime(Instant.now()).graph


  def mapVertices[T: ClassTag](map: (VertexId, Attributes) => T): Graph[T, SnapshotEdgePayload] =
    graph.mapVertices(map)

  def mapEdges[T: ClassTag](map: Edge[SnapshotEdgePayload] => T): Graph[Attributes, T] =
    graph.mapEdges(map)

  def reverse: Graph[Attributes, SnapshotEdgePayload] = graph.reverse


  def snapshotAtTime(instant: Instant): Snapshot

  def subgraph(
                epred: EdgeTriplet[Attributes, SnapshotEdgePayload] => Boolean = (x => true),
                vpred: (VertexId, Attributes) => Boolean = ((v, d) => true))
  : Graph[Attributes, SnapshotEdgePayload]
  = graph.subgraph(epred, vpred)

  def aggregateMessages[A: ClassTag](
                                      sendMsg: EdgeContext[Attributes, SnapshotEdgePayload, A] => Unit,
                                      mergeMsg: (A, A) => A,
                                      tripletFields: TripletFields = TripletFields.All)
  : VertexRDD[A] =
    graph.aggregateMessages(sendMsg, mergeMsg, tripletFields)


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
   * @param timestamp Instant
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
