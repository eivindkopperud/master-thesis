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


class Landy(graph: LandyAttributeGraph) extends TemporalGraph[LandyVertexPayload, LandyEdgePayload] {
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

object Landy {
  def createEdge(log: LogTSV, validTo: Instant): Edge[LandyEdgePayload] = {
    log.entity match {
      case Entity.VERTEX(_) => throw new IllegalStateException("This should not be called on a log with vertices")
      case Entity.EDGE(id, srcId, dstId) => {
        val payload = LandyEdgePayload(id = id, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        Edge(srcId, dstId, payload)
      }
    }
  }

  def createVertex(log: LogTSV, validTo: Instant): (VertexId, LandyVertexPayload) = {
    log.entity match {
      case Entity.VERTEX(objId) => {
        val payload = LandyVertexPayload(id = objId, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        (UtilsUtils.uuid, payload)
      }
      case Entity.EDGE(_, _, _) => throw new IllegalStateException("This should not be called on a log with edges")
    }
  }

  def generateEdges(logs: Seq[LogTSV]): Seq[Edge[LandyEdgePayload]] = {
    val reversedLogs = LogUtils.reverse(logs)
    val aVeryBigDate = Instant.MAX
    var lastTimestamp = aVeryBigDate
    val edges = mutable.MutableList[Edge[LandyEdgePayload]]()
    for (log <- reversedLogs) {
      // DELETES should not create an edge, but instead record the deletion timestamp to be used later.
      if (log.action == CREATE || log.action == UPDATE) {
        edges += createEdge(log, lastTimestamp)
      }
      lastTimestamp = log.timestamp
    }
    edges
  }

  def generateVertices(logs: Seq[LogTSV]): Seq[(VertexId, LandyVertexPayload)] = {
    val reversedLogs = LogUtils.reverse(logs)
    val aVeryBigDate = Instant.MAX
    var lastTimestamp = aVeryBigDate
    val vertices = mutable.MutableList[(VertexId, LandyVertexPayload)]()
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