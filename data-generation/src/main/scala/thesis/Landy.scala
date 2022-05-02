package thesis

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeRDD, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.Action.{CREATE, UPDATE}
import thesis.DataTypes.{Attributes, EdgeId, LandyAttributeGraph}
import utils.{EntityFilterException, LogUtils, UtilsUtils}

import java.time.Instant
import scala.collection.mutable
import scala.math.Ordered.orderingToOrdered


class Landy(graph: LandyAttributeGraph) extends TemporalGraph[LandyEntityPayload, LandyEntityPayload] {
  override val vertices: VertexRDD[LandyEntityPayload] = graph.vertices
  override val edges: EdgeRDD[LandyEntityPayload] = graph.edges
  override val triplets: RDD[EdgeTriplet[LandyEntityPayload, LandyEntityPayload]] = graph.triplets

  override def snapshotAtTime(instant: Instant): Graph[LandyEntityPayload, LandyEntityPayload] = {
    val vertices = localVertices
      .filter(vertex =>
        vertex._2.validFrom < instant &&
          vertex._2.validTo >= instant
      )
      .map(vertex => (vertex._2.id, vertex._2))

    val edges = this.edges.filter(edge =>
      edge.attr.validFrom < instant &&
        edge.attr.validTo >= instant
    )
    Graph(vertices, edges)
  }

  override def directNeighbours(vertexId: VertexId, interval: Interval): RDD[VertexId] = {
    this.edges.filter(edge => {
      val edgeInterval = Interval(edge.attr.validFrom, edge.attr.validTo)
      edge.srcId == vertexId && interval.overlaps(edgeInterval)
    }
    )
      .map(_.dstId)
  }

  override def activatedVertices(interval: Interval): RDD[VertexId] = {
    val firstLocalVertices = localVertices // Remove global vertices
      .map(vertex => (vertex._2.id, vertex._2)) // Get the global vertex ID
      .groupByKey() // Group all
      .map(vertex => (vertex._1, getEarliest(vertex._2.toSeq))) // Get first local vertex

    firstLocalVertices.filter(vertex => interval.contains(vertex._2.validFrom))
      .map(vertex => vertex._2.id)
  }

  override def activatedEdges(interval: Interval): RDD[EdgeId] = {
    this.edges
      .map(edge => (edge.attr.id, edge.attr))
      .groupByKey()
      .map(edge => (edge._1, getEarliest(edge._2.toSeq)))
      .filter(edge => interval.contains(edge._2.validFrom))
      .map(edge => edge._1)
  }

  /**
   * @param payloads Payload updates for a single entity
   * @return The data that corresponds to the earliest timestamp
   */
  private def getEarliest(payloads: Seq[LandyEntityPayload]): LandyEntityPayload = {
    payloads.minBy(_.validFrom)
  }

  /**
   * Get the local vertices, i.e. those who have a payload.
   * Global vertices, which have no payload, are created when edges connect
   * vertices that don't exist from before.
   *
   * @return The local vertices of the graph
   */
  private def localVertices: VertexRDD[LandyEntityPayload] = {
    this.vertices.filter(vertex => vertex._2 != null)
  }

  /** Return entity based on ID at a timestamp
   *
   * @param entity    Edge or Vertex
   * @param timestamp Insant
   * @return Entity Object and HashMap
   */
  override def getEntity[T <: Entity](entity: T, timestamp: Instant): Option[(T, Attributes)] = ???
}

object Landy {
  def createEdge(log: LogTSV, validTo: Instant): Edge[LandyEntityPayload] = {
    log.entity match {
      case VERTEX(_) => throw new EntityFilterException
      case EDGE(id, srcId, dstId) => {
        val payload = LandyEntityPayload(id = id, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        Edge(srcId, dstId, payload)
      }
    }
  }

  def createVertex(log: LogTSV, validTo: Instant): (VertexId, LandyEntityPayload) = {
    log.entity match {
      case VERTEX(id) => {
        val payload = LandyEntityPayload(id = id, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        (UtilsUtils.uuid, payload)
      }
      case EDGE(_, _, _) => throw new EntityFilterException
    }
  }

  def generateEdges(logs: Seq[LogTSV]): Seq[Edge[LandyEntityPayload]] = {
    val reversedLogs = LogUtils.reverse(logs)
    val aVeryBigDate = Instant.MAX
    var lastTimestamp = aVeryBigDate
    val edges = mutable.MutableList[Edge[LandyEntityPayload]]()
    for (log <- reversedLogs) {
      // DELETES should not create an edge, but instead record the deletion timestamp to be used later.
      if (log.action == CREATE || log.action == UPDATE) {
        edges += createEdge(log, lastTimestamp)
      }
      lastTimestamp = log.timestamp
    }
    edges
  }

  def generateVertices(logs: Seq[LogTSV]): Seq[(VertexId, LandyEntityPayload)] = {
    val reversedLogs = LogUtils.reverse(logs)
    val aVeryBigDate = Instant.MAX
    var lastTimestamp = aVeryBigDate
    val vertices = mutable.MutableList[(VertexId, LandyEntityPayload)]()
    for (log <- reversedLogs) {
      // DELETES should not create a vertex, but instead record the deletion timestamp to be used later.
      if (log.action == CREATE || log.action == UPDATE) {
        vertices += createVertex(log, lastTimestamp)
      }
      lastTimestamp = log.timestamp
    }
    vertices
  }

  def apply(logs: RDD[LogTSV])(implicit sc: SparkContext): Landy = {
    val edges = LogUtils.groupEdgeLogsById(logs)
      .flatMap(actionsByEdge => generateEdges(actionsByEdge._2.toSeq))
    val vertices = LogUtils.groupVertexLogsById(logs)
      .flatMap(actionsByVertex => generateVertices(actionsByVertex._2.toSeq))

    val graph = Graph(vertices, edges)
    new Landy(graph)
  }
}
