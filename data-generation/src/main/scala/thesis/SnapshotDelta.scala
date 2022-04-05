package thesis

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import thesis.Action.{CREATE, DELETE, UPDATE}
import thesis.Entity.{EDGE, VERTEX}
import thesis.LTSV.Attributes

import java.time.Instant
import scala.collection.mutable.MutableList
import scala.reflect.ClassTag

abstract class TemporalGraph[VD: ClassTag, ED: ClassTag] extends Serializable {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
  val triplets: RDD[EdgeTriplet[VD, ED]]

  def snapshotAtTime(instant: Instant): Graph[VD, ED]
}

class SnapshotDelta(val graphs: MutableList[(Graph[Attributes, Attributes], Instant)],
                    val logs: RDD[LogTSV],
                    val snapshotType: SnapshotIntervalType) extends TemporalGraph[Attributes, Attributes] { // extends Graph[VD, ED] {
  override val vertices: VertexRDD[Attributes] = graphs.get(0).get._1.vertices
  override val edges: EdgeRDD[Attributes] = graphs.get(0).get._1.edges
  override val triplets: RDD[EdgeTriplet[Attributes, Attributes]] = graphs.get(0).get._1.triplets

class SnapshotDelta(val graphs: MutableList[Graph[Attributes, Attributes]], val logs: RDD[LogTSV], snapshotType: SnapshotIntervalType) { // extends Graph[VD, ED] {
  val vertices: VertexRDD[Attributes] = graphs.get(0).get.vertices
  val edges: EdgeRDD[Attributes] = graphs.get(0).get.edges
  val triplets: RDD[EdgeTriplet[Attributes, Attributes]] = graphs.get(0).get.triplets
  /*
    override def persist(newLevel: StorageLevel): Graph[VD, ED] = graph.persist(newLevel)

    override def cache(): Graph[VD, ED] = graph.cache

    override def checkpoint(): Unit = graph.checkpoint

    override def isCheckpointed: Boolean = graph.isCheckpointed

    override def getCheckpointFiles: Seq[String] = graph.getCheckpointFiles

    override def unpersist(blocking: Boolean): Graph[VD, ED] = graph.unpersist(blocking)

    override def unpersistVertices(blocking: Boolean): Graph[VD, ED] = graph.unpersistVertices(blocking)

    override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = graph.partitionBy(partitionStrategy)

    override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: PartitionID): Graph[VD, ED] = graph.partitionBy(partitionStrategy, numPartitions)

    override def mapVertices[VD2](map: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2], eq: VD =:= VD2): Graph[VD2, ED] = graph.mapVertices(map)(evidence$3, eq)

    override def mapEdges[ED2](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])(implicit evidence$5: ClassTag[ED2]): Graph[VD, ED2] = graph.mapEdges(map)(evidence$5)

    override def mapTriplets[ED2](map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], tripletFields: TripletFields)(implicit evidence$8: ClassTag[ED2]): Graph[VD, ED2] =
      graph.mapTriplets(map, tripletFields)(evidence$8)

    override def reverse: Graph[VD, ED] = graph.reverse

    override def subgraph(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = graph.subgraph(epred, vpred)

    override def mask[VD2, ED2](other: Graph[VD2, ED2])(implicit evidence$9: ClassTag[VD2], evidence$10: ClassTag[ED2]): Graph[VD, ED] = graph.mask(other)(evidence$9, evidence$10)

    override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = graph.groupEdges(merge)

    override def outerJoinVertices[U, VD2](other: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit evidence$13: ClassTag[U], evidence$14: ClassTag[VD2], eq: VD =:= VD2): Graph[VD2, ED] =
      graph.outerJoinVertices(other)(mapFunc)(evidence$13, evidence$14, eq)
   */
}


sealed abstract class SnapshotIntervalType

object SnapshotIntervalType {
  final case class Time(duration: Int) extends SnapshotIntervalType

  final case class Count(numberOfActions: Int) extends SnapshotIntervalType
}

object SnapshotDeltaObject {
  def getLogger: Logger = LoggerFactory.getLogger("SnapShotDeltaObject")

  def create[VD, ED](logs: RDD[LogTSV], snapType: SnapshotIntervalType): SnapshotDelta = {
    snapType match {
      // TODO the first argument logsWithSortableKey should be moved into the function
      case SnapshotIntervalType.Time(duration) => createSnapshotModel(logs.map(log => (log.timestamp.getEpochSecond, log)), snapType)
      case SnapshotIntervalType.Count(numberOfActions) => createSnapshotModel(logs.zipWithIndex().map(x => (x._2, x._1)), snapType)
    }
  }

  /** Create snapshotDelta model
   *
   * This function works for both a timed and a counter based model.
   *
   * There is maybe too many mins and maxs that can have a performance hit
   *
   * @param logsWithSortableKey  Logs with a sortable key, either seconds or amount of actions
   * @param snapshotIntervalType The type of snapshot
   * @return SnapShotDelta object
   */
  def createSnapshotModel(logsWithSortableKey: RDD[(Long, LogTSV)], snapshotIntervalType: SnapshotIntervalType): SnapshotDelta = {
    getLogger.warn(s"Creating SnapshotModel with type:$snapshotIntervalType")
    val max = logsWithSortableKey.map(_._1).max() // This will value is only used for Count, but it should be lazy, so its fine
    val min = logsWithSortableKey.map(_._1).min() // This will be 0 for Count and the earliest timestamp for Time

    val (numberOfSnapShots, interval) = snapshotIntervalType match {
      case SnapshotIntervalType.Time(duration) => (((max - min).toFloat / duration).ceil.toInt, duration)
      case SnapshotIntervalType.Count(number) => ((logsWithSortableKey.count().toFloat / number).ceil.toInt, number) // .count() should in _theory_ be the same as 'max'. So these two lines could in theory be identical
    }

    val initLogs = logsWithSortableKey.filterByRange(min, interval).map(_._2)
    val initTimestamp = initLogs.map(_.timestamp).max()
    val initialGraph = createGraph(initLogs)
    val graphs = MutableList((initialGraph, initTimestamp))

    for (i <- 1 until numberOfSnapShots) {
      val previousSnapshot = graphs(i - 1)

      // Get subset of logs
      val logInterval = logsWithSortableKey
        .filterByRange(min + (interval * i), min + (interval * (i + 1)) - 1) // This lets ut get a slice of the LTSVs (filterByRange is inclusive)
        .map(_._2) // This maps the type into back to its original form

      val newVertices = applyVertexLogsToSnapshot(previousSnapshot._1, logInterval)
      val newEdges = applyEdgeLogs(previousSnapshot._1, logInterval)

      val graphTimestamp = Try {
        logInterval.map(_.timestamp).max()
      }.getOrElse(Instant.ofEpochSecond(min + (interval * i + 1) - 1)) //TODO write test to make sure the timestamps are correct
      val newGraphWithTimestamp = (Graph(newVertices, newEdges), graphTimestamp)
      graphs += newGraphWithTimestamp
    }
    new SnapshotDelta(graphs, logsWithSortableKey.map(_._2), snapshotIntervalType)
  }

  def applyEdgeLogs(snapshot: Graph[LTSV.Attributes, LTSV.Attributes], logs: RDD[LogTSV]): RDD[Edge[Attributes]] = {
    val previousSnapshotEdgesKV = snapshot.edges.map(edge => ((edge.dstId, edge.srcId), edge.attr))

    val squashedEdgeActions = getSquashedActionsByEdgeId(logs)

    // Combine edges from the snapshot with the current log interval
    val joinedEdges = previousSnapshotEdgesKV.fullOuterJoin(squashedEdgeActions)

    // Create, update or omit (delete) edges based on values are present or not.
    val newSnapshotEdges = joinedEdges.flatMap(outerJoinedEdge => outerJoinedEdge._2 match {
      case (None, None) => throw new IllegalStateException("The full outer join f*ucked up")
      case (Some(previousEdge), None) => Some(Edge(outerJoinedEdge._1._1, outerJoinedEdge._1._2, previousEdge)) // Edge existed in the last snapshot, but no changes were done in this interval
      case (Some(previousEdge), Some(newEdgeAction)) => newEdgeAction.action match { // There has been a change to the edge in the interval
        case DELETE => None // Edge or {source,destination} vertex was deleted in the new interval
        case UPDATE => // Edge updated in this interval
          Some(Edge(outerJoinedEdge._1._1, outerJoinedEdge._1._2, rightWayMergeHashMap(previousEdge, newEdgeAction.attributes)))
        case CREATE =>
          throw new IllegalStateException("An CREATE should never happen since this case should be an action referencing an existing entity")
      }
      case (None, Some(newEdgeAction)) => newEdgeAction.action match { // Edge was introduced in this interval
        case DELETE => None // Edge was introduced then promptly deleted (possibly due to a deleted vertex)
        case CREATE => // Edge was created
          Some(Edge(outerJoinedEdge._1._1, outerJoinedEdge._1._2, newEdgeAction.attributes))
        case UPDATE =>
          throw new IllegalStateException("An UPDATE should never happen if it's new because of how merging of LogTSVs are done")
      }
    })
    newSnapshotEdges
  }

  // We are making the assumption that there can only be one edge between two nodes. We might have to create a surrogate key
  def getSquashedActionsByEdgeId(logs: RDD[LogTSV]): RDD[((Long, Long), LogTSV)] = {
    // Filter out vertex actions
    val edgeIdWithEdgeActions = logs.flatMap(log => log.entity match {
      case VERTEX(_) => None
      case EDGE(srcId, dstId) => Some((srcId, dstId), log)
    })

    // Group by edge id and merge
    edgeIdWithEdgeActions.groupByKey()
      .map(edgeWithActions => (edgeWithActions._1, mergeLogTSVs(edgeWithActions._2)))
  }

  def applyVertexLogsToSnapshot(snapshot: Graph[LTSV.Attributes, LTSV.Attributes], logs: RDD[LogTSV]): RDD[(VertexId, Attributes)] = {
    val previousSnapshotVertices = snapshot.vertices.map(vertex => (vertex._1, vertex._2))

    val vertexIDsWithAction: RDD[(Long, LogTSV)] = getSquashedActionsByVertexId(logs)

    // Combine vertices from the snapshot with the current log interval
    val joinedVertices = previousSnapshotVertices.fullOuterJoin(vertexIDsWithAction)

    // Create, update or omit (delete) vertices based on values are present or not.
    val newSnapshotVertices = joinedVertices.flatMap(outerJoinedVertex => outerJoinedVertex._2 match {
      case (None, None) => throw new IllegalStateException("The full outer join f*ucked up")
      case (Some(snapshotVertex), None) => Some((outerJoinedVertex._1, snapshotVertex)) // No changes to the vertex in this interval
      case (Some(snapshotVertex), Some(newVertex)) => newVertex.action match { // Changes to the vertex in this interval
        case DELETE => None // Vertex was deleted
        case UPDATE => // Vertex was updated
          Some((outerJoinedVertex._1, rightWayMergeHashMap(snapshotVertex, newVertex.attributes)))
        case CREATE => throw new IllegalStateException("An CREATE should never happen since this case should be an action referencing an existing entity")
      }
      case (None, Some(newVertex)) => newVertex.action match { // Vertex was introduced in this interval
        case DELETE => None // Vertex was introduced then promptly deleted
        case CREATE => // Vertex was created
          Some((outerJoinedVertex._1, newVertex.attributes))
        case UPDATE =>
          throw new IllegalStateException(s"An UPDATE should never happen if the vertex is new in the interval, because of the way merging og LogTSVs are done: Vertex $newVertex")
      }
    })
    newSnapshotVertices
  }

  def getSquashedActionsByVertexId(logs: RDD[LogTSV]): RDD[(Long, LogTSV)] = {
    // Filter out edge actions
    val vertexIdWithVertexActions = logs.flatMap(log => log.entity match {
      case VERTEX(objId) => Some(objId, log)
      case EDGE(_, _) => None
    })

    // Group by vertex id and merge
    vertexIdWithVertexActions.groupByKey()
      .map(vertexWithActions => (vertexWithActions._1, mergeLogTSVs(vertexWithActions._2)))
  }

  def mergeLogTSVs(logs: Iterable[LogTSV]): LogTSV = logs.reduce(mergeLogTSV)

  def mergeLogTSV(prevLog: LogTSV, nextLog: LogTSV): LogTSV = {
    (prevLog.action, nextLog.action) match {
      case (UPDATE, DELETE) => nextLog // DELETE nullifies previous operations
      case (CREATE, DELETE) => nextLog // DELETE nullifies previous operations
      case (UPDATE, UPDATE) =>
        nextLog.copy(attributes = rightWayMergeHashMap(prevLog.attributes, nextLog.attributes)) // Could be either l1 or l2 that is copied
      case (CREATE, UPDATE) =>
        prevLog.copy(attributes = rightWayMergeHashMap(prevLog.attributes, nextLog.attributes))
      case (prevLogState, nextLogState) => throw new IllegalStateException(s"($prevLogState,$nextLogState encountered when merging logs. The dataset is inconsistent")
      // Cases that should not happen. We assume the LogTSVs are consistent and makes sense
      // example (DELETE, DELETE), (DELETE, UPDATE) // These do not make sense
    }
  }

  /** Merge two hashmaps with preference for dominantAttributes
   *
   * Attributes is type alias for HashMap[(String,String)]
   *
   * @param attributes         recessive HashMap
   * @param dominantAttributes dominant HashMap
   * @return merged HashMap
   */
  def rightWayMergeHashMap(attributes: LTSV.Attributes, dominantAttributes: LTSV.Attributes): LTSV.Attributes = {
    attributes.merged(dominantAttributes)((_, y) => y)
  }

  def getEdgeIds(log: LogTSV): Option[(Long, Long)] = {
    log.entity match {
      case VERTEX(_) => None
      case EDGE(srcId, dstId) => Some(srcId, dstId)
    }
  }

  def getVertexIds(log: LogTSV): Option[Long] = {
    log.entity match {
      case VERTEX(objId) => Some(objId)
      case EDGE(_, _) => None
    }
  }

  /** Create graph from an initial list of LogTSVs
   *
   * @param logs Initial log entries
   * @return new Graph
   */
  def createGraph(logs: RDD[LogTSV]): Graph[Attributes, Attributes] = {
    getLogger.warn("Creating graph from RDD[LogTSV]")
    val firstSnapshotVertices = getInitialVerticesFromLogs(logs)
    val firstSnapshotEdges = getInitialEdgesFromLogs(logs)

    Graph(firstSnapshotVertices, firstSnapshotEdges)
  }

  def getInitialVerticesFromLogs(logs: RDD[LogTSV]): RDD[(VertexId, Attributes)] = {
    val verticesWithAction = getSquashedActionsByVertexId(logs)
    verticesWithAction.flatMap(vertexTuple => vertexTuple._2.action match {
      case CREATE => Some(vertexTuple._1, vertexTuple._2.attributes) // Vertex was created
      case DELETE => None // Vertex was created and deleted
      case UPDATE => Some(vertexTuple._1, vertexTuple._2.attributes) // Vertex was created and updated
    })
  }

  def getInitialEdgesFromLogs(logs: RDD[LogTSV]): RDD[Edge[Attributes]] = {
    val edgesWithAction = getSquashedActionsByEdgeId(logs)
    edgesWithAction.flatMap(edge => edge._2.action match {
      case CREATE => Some(Edge(edge._1._1, edge._1._2, edge._2.attributes)) // Edge was created
      case DELETE => None // Edge was created and deleted (Possibly because of a deleted vertex)
      case UPDATE => Some(Edge(edge._1._1, edge._1._2, edge._2.attributes)) // Edge was created and updated
    })
  }

}
