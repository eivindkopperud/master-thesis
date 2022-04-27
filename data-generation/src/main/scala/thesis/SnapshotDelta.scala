package thesis

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.slf4j.{Logger, LoggerFactory}
import thesis.Action.{CREATE, DELETE, UPDATE}
import thesis.Entity.{EDGE, VERTEX}
import thesis.LTSV.{Attributes, EdgeId}
import thesis.SnapshotDeltaObject._

import java.time.{Duration, Instant}
import scala.collection.mutable.MutableList
import scala.math.Ordered.orderingToOrdered
import scala.reflect.ClassTag
import scala.util.Try


//Should just be a trait
// TODO refactor this into its own file, snapshotDelta is a very big file ATM
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
}

case class SnapshotEdgePayload(id: EdgeId, attributes: Attributes)

//TODO have file with all the finurlige case classes
case class Interval(start: Instant, stop: Instant)

class SnapshotDelta(val graphs: MutableList[Snapshot],
                    val logs: RDD[LogTSV],
                    val snapshotType: SnapshotIntervalType) extends TemporalGraph[Attributes, SnapshotEdgePayload] {
  override val vertices: VertexRDD[Attributes] = graphs.get(0).get.graph.vertices
  override val edges: EdgeRDD[SnapshotEdgePayload] = graphs.get(0).get.graph.edges
  override val triplets: RDD[EdgeTriplet[Attributes, SnapshotEdgePayload]] = graphs.get(0).get.graph.triplets
  val logger: Logger = getLogger

  def forwardApplyLogs(graph: AttributeGraph, logsToApply: RDD[LogTSV]): AttributeGraph = {
    Graph(
      applyVertexLogsToSnapshot(graph, logsToApply),
      applyEdgeLogsToSnapshot(graph, logsToApply)
    )
  }

  override def activatedEntities(interval: Interval): (RDD[VertexId], RDD[EdgeId]) = {
    val relevantLogs = getLogsInInterval(logs, interval)
    val (vertexSquash, edgeSquash) = getSquashedActionsByEntityId(relevantLogs)
    val activatedVertices = vertexSquash.filter(_._2.action == CREATE).map(_._1)
    val activatedEdges = edgeSquash.filter(_._2.action == CREATE).map(_._1)
    (activatedVertices, activatedEdges)
  }

  override def snapshotAtTime(instant: Instant): AttributeGraph = {

    //Closest graph should be based on number of logs, not time.
    // But for it to be feasible each graph should have a sequence ID as well
    // This is possible, I think, but not implemented
    val closestGraph = graphs.reduceLeft((snap1, snap2) =>
      if (snap2.instant > instant) { 
        snap1
      } else {
        snap2
      })
    logger.warn(s"Instant $instant, Closest :graph${closestGraph.instant}")
    if (closestGraph.instant == instant) {
      logger.warn("The queried graph is already materialized")
      closestGraph.graph
    } else if (instant < closestGraph.instant) { // We are asking an instant before the initial materialized snapshot
      logger.warn("Closest graph in the future, and there is no one in the past.")
      createGraph(logs.map(l => (l.timestamp, l)).filterByRange(Instant.MIN, instant).map(_._2))
    } else {
      logger.warn("Closest graph is in the past. Need to apply logs forwards")
      val logsToApply = logs.map(l => (l.timestamp, l)).filterByRange(closestGraph.instant, instant).map(_._2)
      forwardApplyLogs(closestGraph.graph, logsToApply)
    }
  }


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

final case class Snapshot(graph: AttributeGraph, instant: Instant)

object SnapshotDeltaObject {
  def getLogger: Logger = LoggerFactory.getLogger("SnapShotDelta")

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

    val initLogs = logsWithSortableKey.filterByRange(min, interval - 1).map(_._2)
    val initTimestamp = initLogs.map(_.timestamp).max()
    val initialGraph = createGraph(initLogs)
    val graphs = MutableList(Snapshot(initialGraph, initTimestamp))

    for (i <- 1 until numberOfSnapShots) {
      val previousSnapshot = graphs(i - 1)

      // Get subset of logs
      val logInterval = logsWithSortableKey
        .filterByRange(min + (interval * i), min + (interval * (i + 1)) - 1) // This lets ut get a slice of the LTSVs (filterByRange is inclusive)
        .map(_._2) // This maps the type into back to its original form

      val newVertices = applyVertexLogsToSnapshot(previousSnapshot.graph, logInterval)
      val newEdges = applyEdgeLogsToSnapshot(previousSnapshot.graph, logInterval)

      val graphTimestamp = Try {
        logInterval.map(_.timestamp).max()
      }.getOrElse(Instant.ofEpochSecond(min + (interval * i + 1) - 1)) //TODO write test to make sure the timestamps are correct
      val newGraphWithTimestamp = Snapshot(Graph(newVertices, newEdges), graphTimestamp)
      graphs += newGraphWithTimestamp
    }
    new SnapshotDelta(graphs, logsWithSortableKey.map(_._2), snapshotIntervalType)
  }

  def updateEdgeWithLog(edge: Edge[SnapshotEdgePayload], log: LogTSV): Edge[SnapshotEdgePayload] = {
    edge
      .copy(attr =
        edge.attr
          .copy(attributes = rightWayMergeHashMap(edge.attr.attributes, log.attributes)))
  }

  def getLogsInInterval(logs: RDD[LogTSV], interval: Interval): RDD[LogTSV] = {
    logs.map(log => (log.timestamp, log)).filterByRange(interval.start, interval.stop).map(_._2)
  }

  def applyEdgeLogsToSnapshot(snapshot: AttributeGraph, logs: RDD[LogTSV]): RDD[Edge[SnapshotEdgePayload]] = {
    val uuid = java.util.UUID.randomUUID.toString.take(5)
    val previousSnapshotEdgesKV = snapshot.edges.map(edge => (edge.attr.id, edge))

    val squashedEdgeActions = getSquashedActionsByEdgeId(logs)

    // Combine edges from the snapshot with the current log interval
    val joinedEdges = previousSnapshotEdgesKV.fullOuterJoin(squashedEdgeActions)

    // Create, update or omit (delete) edges based on values are present or not.
    val newSnapshotEdges: RDD[Edge[SnapshotEdgePayload]] = joinedEdges.flatMap(outerJoinedEdge => outerJoinedEdge._2 match {
      case (None, None) => throw new IllegalStateException("The full outer join f*ucked up")
      case (Some(previousEdge), None) => Some(Edge(previousEdge.srcId, previousEdge.dstId, previousEdge.attr)) // Edge existed in the last snapshot, but no changes were done in this interval
      case (Some(previousEdge), Some(log)) => log.action match { // There has been a change to the edge in the interval
        case DELETE => None // Edge or {source,destination} vertex was deleted in the new interval
        case UPDATE => // Edge updated in this interval
          Some(updateEdgeWithLog(previousEdge, log))
        case CREATE =>
          throw new IllegalStateException(s"$uuid An CREATE should never happen since this case should be an action referencing an existing entity")
      }
      case (None, Some(newEdgeAction)) => newEdgeAction.action match { // Edge was introduced in this interval
        case DELETE => None // Edge was introduced then promptly deleted (possibly due to a deleted vertex)
        case CREATE => // Edge was created
          Some(logToEdge(newEdgeAction))
        case UPDATE =>
          throw new IllegalStateException(s"$uuid An UPDATE should never happen if it's new because of how merging of LogTSVs are done $newEdgeAction")
      }
    })
    newSnapshotEdges
  }

  def logToEdge(log: LogTSV): Edge[SnapshotEdgePayload] = {
    log.entity match {
      case VERTEX(objId) => throw new IllegalStateException("This does not make sense. Only edges allowed")
      case EDGE(id, srcId, dstId) => Edge(srcId, dstId, SnapshotEdgePayload(id, log.attributes))
    }
  }

  def getSquashedActionsByEdgeId(logs: RDD[LogTSV]): RDD[(EdgeId, LogTSV)] = {
    // Filter out vertex actions
    val edgeIdWithEdgeActions = logs.flatMap(log => log.entity match {
      case _: VERTEX => None
      case EDGE(id, _, _) => Some(id, log)
    })

    // Group by edge id and merge
    edgeIdWithEdgeActions.groupByKey()
      .map(edgeWithActions => (edgeWithActions._1, mergeLogTSVs(edgeWithActions._2)))
  }

  def applyVertexLogsToSnapshot(snapshot: AttributeGraph, logs: RDD[LogTSV]): RDD[(VertexId, Attributes)] = {
    val uuid = java.util.UUID.randomUUID.toString.take(5)
    val previousSnapshotVertices = snapshot.vertices

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
        case CREATE => throw new IllegalStateException(s"$uuid An CREATE should never happen since this case should be an action referencing an existing vertex: $snapshotVertex $newVertex ")
      }
      case (None, Some(newVertex)) => newVertex.action match { // Vertex was introduced in this interval
        case DELETE => None // Vertex was introduced then promptly deleted
        case CREATE => // Vertex was created
          Some((outerJoinedVertex._1, newVertex.attributes))
        case UPDATE =>
          throw new IllegalStateException(s"$uuid An UPDATE should never happen if the vertex is new in the interval, because of the way merging og LogTSVs are done: Vertex $newVertex")
      }
    })
    newSnapshotVertices
  }

  def getSquashedActionsByVertexId(logs: RDD[LogTSV]): RDD[(Long, LogTSV)] = {
    // Filter out edge actions
    val vertexIdWithVertexActions = logs.flatMap(log => log.entity match {
      case VERTEX(objId) => Some(objId, log)
      case _: EDGE => None
    })

    // Group by vertex id and merge
    vertexIdWithVertexActions.groupByKey()
      .map(vertexWithActions => (vertexWithActions._1, mergeLogTSVs(vertexWithActions._2)))
  }

  def getSquashedActionsByEntityId(logs: RDD[LogTSV]): (RDD[(VertexId, LogTSV)], RDD[(EdgeId, LogTSV)]) = (getSquashedActionsByVertexId(logs), getSquashedActionsByEdgeId(logs))

  def mergeLogTSVs(logs: Iterable[LogTSV]): LogTSV = logs.reduce(mergeLogTSV)

  def mergeLogTSV(prevLog: LogTSV, nextLog: LogTSV): LogTSV = {
    (prevLog.action, nextLog.action) match {
      case (UPDATE, DELETE) => nextLog // DELETE nullifies previous operations
      case (CREATE, DELETE) => nextLog // DELETE nullifies previous operations
      case (UPDATE, UPDATE) =>
        nextLog.copy(attributes = rightWayMergeHashMap(prevLog.attributes, nextLog.attributes)) // Could be either l1 or l2 that is copied
      case (CREATE, UPDATE) =>
        prevLog.copy(attributes = rightWayMergeHashMap(prevLog.attributes, nextLog.attributes))
      case (prevLogState, nextLogState) => throw new IllegalStateException(s"($prevLogState,$nextLogState encountered when merging logs. The dataset is inconsistent \n Prev: $prevLog \n Next: $nextLog")
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

  /** Create graph from an initial list of LogTSVs
   *
   * @param logs Initial log entries
   * @return new Graph
   */
  def createGraph(logs: RDD[LogTSV]): AttributeGraph = {
    getLogger.warn("Creating (most likely) initial graph from RDD[LogTSV]")
    val firstSnapshotVertices = getInitialVerticesFromLogs(logs)
    val firstSnapshotEdges = getInitialEdgesFromLogs(logs)

    Graph(firstSnapshotVertices, firstSnapshotEdges)
  }

  def getInitialVerticesFromLogs(logs: RDD[LogTSV]): RDD[(VertexId, Attributes)] = {
    val verticesWithAction = getSquashedActionsByVertexId(logs)
    verticesWithAction.flatMap(vertexTuple => vertexTuple._2.action match {
      case CREATE => Some(vertexTuple._1, vertexTuple._2.attributes) // Vertex was created
      case DELETE => None // Vertex was created and deleted
      case UPDATE => throw new IllegalStateException("This should never happen based on the way we do merging")
      // This last case UPDATE should never happen because of the way we do merging?
    })
  }

  def getInitialEdgesFromLogs(logs: RDD[LogTSV]): RDD[Edge[SnapshotEdgePayload]] = {
    val edgesWithAction = getSquashedActionsByEdgeId(logs)
    edgesWithAction.flatMap(edge => edge._2.action match {
      case CREATE => Some(logToEdge(edge._2)) // Edge was created
      case DELETE => None // Edge was created and deleted (Possibly because of a deleted vertex)
      case UPDATE => throw new IllegalStateException("This should never happen based on the way we do merging")
      // This last case UPDATE should never happen because of the way we do merging?
    })
  }

  /** Returns the closest graph for a given instant
   * Currently unused but might be relevant later
   *
   * @param instant   the timestamp
   * @param snapshot1 snapshot1
   * @param snapshot2 snapshot2
   * @return
   */
  def returnClosestGraph(instant: Instant)(snapshot1: Snapshot, snapshot2: Snapshot): Snapshot =
    if (Duration.between(snapshot1.instant, instant).abs() <= Duration.between(snapshot2.instant, instant).abs()) {
      snapshot1
    } else {
      snapshot2
    }

  // This is how a materialized graph should be
  type AttributeGraph = Graph[Attributes, SnapshotEdgePayload]
  type LandyAttributeGraph = Graph[LandyVertexPayload, LandyEdgePayload]


}
