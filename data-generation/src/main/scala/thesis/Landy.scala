package thesis

import org.apache.spark.rdd.RDD
import thesis.DataTypes.{EdgeId, LandyAttributeGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeRDD, EdgeTriplet, Graph, VertexId, VertexRDD}
import thesis.Action.{CREATE, UPDATE}
import utils.{LogUtils, UtilsUtils}

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

  override def activatedVertices(interval: Interval): RDD[VertexId] = {
    val firstLocalVertices = localVertices // Remove global vertices
      .map(vertex => (vertex._2.id, vertex._2)) // Get the global vertex ID
      .groupByKey() // Group all
      .map(vertex => (vertex._1, getEarliest(vertex._2.toSeq))) // Get first local vertex

    firstLocalVertices.filter(vertex => isInstantInInterval(vertex._2.validFrom, interval))
      .map(vertex => vertex._2.id)
  }

  override def activatedEdges(interval: Interval): RDD[EdgeId] = {
    this.edges
      .map(edge => (edge.attr.id, edge.attr))
      .groupByKey()
      .map(edge => (edge._1, getEarliest(edge._2.toSeq)))
      .filter(edge => isInstantInInterval(edge._2.validFrom, interval))
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

  private def isInstantInInterval(instant: Instant, interval: Interval): Boolean = {
    instant >= interval.start && instant <= interval.stop
  }
}

object Landy {
  def createEdge(log: LogTSV, validTo: Instant): Edge[LandyEntityPayload] = {
    log.entity match {
      case Entity.VERTEX(_) => throw new IllegalStateException("This should not be called on a log with vertices")
      case Entity.EDGE(id, srcId, dstId) => {
        val payload = LandyEntityPayload(id = id, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        Edge(srcId, dstId, payload)
      }
    }
  }

  def createVertex(log: LogTSV, validTo: Instant): (VertexId, LandyEntityPayload) = {
    log.entity match {
      case Entity.VERTEX(objId) => {
        val payload = LandyEntityPayload(id = objId, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        (UtilsUtils.uuid, payload)
      }
      case Entity.EDGE(_, _, _) => throw new IllegalStateException("This should not be called on a log with edges")
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
    val edges = LogUtils.getEdgeLogsById(logs)
      .flatMap(actionsByEdge => generateEdges(actionsByEdge._2.toSeq))
    val vertices = LogUtils.getVertexLogsById(logs)
      .flatMap(actionsByVertex => generateVertices(actionsByVertex._2.toSeq))

    val graph = Graph(vertices, edges)
    new Landy(graph)
  }
}