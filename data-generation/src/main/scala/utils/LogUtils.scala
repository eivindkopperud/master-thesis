package utils

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import thesis.Entity.{EDGE, VERTEX}
import thesis.LogTSV

object LogUtils {

  /**
   * Order all logs as key-value pairs, with vertex id as the key and a list of corresponding logs as the value.
   *
   * @param logs Unprocessed logs for both vertices and edges.
   * @return A RDD of all vertices by id along with their respective logs.
   */
  def getVertexLogsById(logs: RDD[LogTSV]): RDD[(VertexId, Iterable[LogTSV])] = {
    // Filter out edge actions
    val vertexIdWithVertexActions = logs.flatMap(log => log.entity match {
      case VERTEX(objId) => Some(objId, log)
      case _: EDGE => None
    })

    // Group by vertex id and merge
    vertexIdWithVertexActions.groupByKey()
  }

  /** Get only the logs relevant for a specific vertex
   *
   * @param logs   logs
   * @param vertex specific vertex
   * @return RDD with the relevant logs
   */
  def filterVertexLogsById(logs: RDD[LogTSV], vertex: VERTEX): RDD[LogTSV] =
    logs.flatMap(log => log.entity match {
      case VERTEX(objId) if objId == vertex.objId => Some(log)
      case _: EDGE => None
    })

  /**
   * Order all logs as key-value pairs, with edge ids as the key and a list of corresponding logs as the value.
   *
   * @param logs logs
   * @return A RDD of all edges by id along with their respective logs.
   */
  def getEdgeLogsById(logs: RDD[LogTSV]): RDD[(Long, Iterable[LogTSV])] = {
    // Filter out vertex actions
    val edgeIdWithEdgeActions = logs.flatMap(log => log.entity match {
      case _: VERTEX => None
      case EDGE(id, _, _) => Some(id, log)
    })

    // Group by edge id and merge
    edgeIdWithEdgeActions.groupByKey()
  }

  def reverse(logs: Seq[LogTSV]): Seq[LogTSV] = {
    logs.sortWith((log1, log2) => log1.timestamp.isAfter(log2.timestamp))
  }
}
