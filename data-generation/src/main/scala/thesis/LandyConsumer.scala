package thesis

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import thesis.Action.{CREATE, UPDATE}
import utils.{LogUtils, UtilsUtils}

import java.time.Instant
import scala.collection.mutable

object LandyConsumer {
  def createEdge(log: LogTSV, validTo: Instant): Edge[LandyEdgePayload] = {
    log.entity match {
      case Entity.VERTEX(_) => throw new IllegalStateException("This should not be called on a log with vertices")
      case Entity.EDGE(srcId, dstId) => {
        val payload = LandyEdgePayload(id=UtilsUtils.uuid, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        Edge(srcId, dstId, payload)
      }
    }
  }

  def createVertex(log: LogTSV, validTo: Instant): (Long, LandyVertexPayload) = {
    log.entity match {
      case Entity.VERTEX(objId) => {
        val payload = LandyVertexPayload(id=objId, validFrom = log.timestamp, validTo = validTo, attributes = log.attributes)
        (UtilsUtils.uuid, payload)
      }
      case Entity.EDGE(_, _) => throw new IllegalStateException("This should not be called on a log with edges")
    }
  }

  def generateEdges(logs: Seq[LogTSV]): Seq[Edge[LandyEdgePayload]] = {
    val backwardOrderedLogs = logs.sortWith((log1, log2) => log1.timestamp.isBefore(log2.timestamp))
    val aVeryBigDate = Instant.parse("9999-01-01T00:00:00.000Z")
    var lastTimestamp = aVeryBigDate
    val edges = mutable.MutableList[Edge[LandyEdgePayload]]()
    for (log <- backwardOrderedLogs) {
      // DELETES should not create an edge, but instead record the deletion timestamp to be used later.
      if (log.action == CREATE || log.action == UPDATE) {
        edges += createEdge(log, lastTimestamp)
      }
      lastTimestamp = log.timestamp
    }
    edges
    }

  def generateVertices(logs: Seq[LogTSV]): Seq[(Long, LandyVertexPayload)] = {
    val backwardOrderedLogs = logs.sortWith((log1, log2) => log1.timestamp.isBefore(log2.timestamp))
    val aVeryBigDate = Instant.parse("9999-01-01T00:00:00.000Z")
    var lastTimestamp = aVeryBigDate
    val vertices = mutable.MutableList[(Long, LandyVertexPayload)]()
    for (log <- backwardOrderedLogs) {
      // DELETES should not create a vertex, but instead record the deletion timestamp to be used later.
      if (log.action == CREATE || log.action == UPDATE) {
        vertices += createVertex(log, lastTimestamp)
      }
      lastTimestamp = log.timestamp
    }
    vertices
  }

  def consume(logs: RDD[LogTSV])(implicit sc: SparkContext): Landy = {
    val edges = LogUtils.getEdgeLogsById(logs)
      .flatMap(actionsByEdge => generateEdges(actionsByEdge._2.toSeq))
    val vertices = LogUtils.getVertexLogsById(logs)
      .flatMap(actionsByVertex => generateVertices(actionsByVertex._2.toSeq))

    val graph = Graph(vertices, edges)
    new Landy(graph)
  }
}
