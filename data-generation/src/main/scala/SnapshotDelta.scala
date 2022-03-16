import Action.{CREATE, DELETE, UPDATE}
import Entity.{EDGE, VERTEX}
import LTSV.{Attributes, deserializeLTSV}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.reflect.ClassTag

class SnapshotDelta[VD: ClassTag, ED: ClassTag](val graphs: MutableList[Graph[Attributes, Attributes]], val logs:RDD[LogTSV]) extends Graph[VD, ED] {
  override val vertices: VertexRDD[VD] = graph.vertices
  override val edges: EdgeRDD[ED] = graph.edges
  override val triplets: RDD[EdgeTriplet[VD, ED]] = graph.triplets

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
}


sealed abstract class SnapshotIntervalType
object SnapshotIntervalType {
  final case class Time(duration:Int) extends SnapshotIntervalType
  final case class Count(numberOfActions:Int) extends SnapshotIntervalType
}

object SnapshotDeltaObject {
  def create[VD,ED](logs: RDD[LogTSV], snapType: SnapshotIntervalType): SnapshotDelta[Attributes, Attributes] = {
    snapType match {
      case SnapshotIntervalType.Time(duration) => snapShotTime(logs, duration)
      case SnapshotIntervalType.Count(numberOfActions) => createSnapShotCountModel(logs, numberOfActions)
    }
  }

  def createSnapShotCountModel(logs: RDD[LogTSV], numberOfActions: Int): SnapshotDelta[Attributes, Attributes] = {
    val initialGraph = createGraph(logs.map(log => (log.sequentialId, log)).filterByRange(0, numberOfActions).map(_._2))
    val graphs = mutable.MutableList(initialGraph)

      for ( i <- 1 to (logs.count() / numberOfActions).ceil.toInt) {
        val previousSnapshot = graphs(i - 1)

        // Get subset of logs
        val logInterval = logs
          .map(log => (log.sequentialId, log))                           // Mapping like this makes it into a KeyValuePairRDD, which have some convenient methods
          .filterByRange(numberOfActions * i, numberOfActions * (i + 1)) // This lets ut get a slice of the LTSVs
          .map(_._2)                                                     // This maps the type into back to its original form

        val newVertices = applyVertexLogsToSnapshot(previousSnapshot, logInterval)
        val newEdges = applyEdgeLogs(previousSnapshot, logInterval)

        graphs += Graph(newVertices, newEdges)
      }
    new SnapshotDelta(graphs, logs)
  }
}
def applyEdgeLogs(snapshot: Graph[LTSV.Attributes, LTSV.Attributes], logs: RDD[LogTSV]): RDD[Edge[Attributes]] = {
  val previousSnapshotEdgesKV = snapshot.edges.map(edge => ((edge.dstId, edge.srcId), edge.attr))

  val squashedEdgeActions = getSquashedActionsByEdgeId(logs)

  // Combine edges from the snapshot with the current log interval
  val joinedEdges = previousSnapshotEdgesKV.fullOuterJoin(squashedEdgeActions)

  // Create, update or omit (delete) edges based on values are present or not.
  val newSnapshotEdges = joinedEdges.flatMap(outerJoinedEdge => outerJoinedEdge._2 match {
    case (Some(previousEdge), None) => Some(Edge(outerJoinedEdge._1._1, outerJoinedEdge._1._2, previousEdge)) // Edge existed in the last snapshot, but no changes were done in this interval
    case (Some(previousEdge), Some(newEdgeAction)) => newEdgeAction match {                 // There has been a change to the edge in the interval
      case newEdgeAction.action == DELETE => None                                           // Edge or {source,destination} vertex was deleted in the new interval
      case newEdgeAction.action == UPDATE =>                                                // Edge updated in this interval
        Some(Edge(outerJoinedEdge._1._1, outerJoinedEdge._1._2, rightWayMergeHashMap(previousEdge, newEdgeAction.attributes)))
    }
    case (None, Some(newEdgeAction)) => newEdgeAction match {                               // Edge was introduced in this interval
      case newEdgeAction.action == DELETE => None                                           // Edge was introduced then promptly deleted (possibly due to a deleted vertex)
      case newEdgeAction.action == CREATE =>                                                // Edge was created
        Some(Edge(outerJoinedEdge._1._1, outerJoinedEdge._1._2, newEdgeAction.attributes))
      case newEdgeAction.action == UPDATE =>                                                // Edge was created and had an update
        Some(Edge(outerJoinedEdge._1._1, outerJoinedEdge._1._2, newEdgeAction.attributes))
    }
  })
  newSnapshotEdges
}

// We are makin the assumption that there can only be one edge between two nodes. We might have to create a surrogate key
def getSquashedActionsByEdgeId(logs: RDD[LogTSV]): RDD[((Long, Long), LogTSV)] = {
  // Get all ids of edges in the log interval
  val edgeIds = logs.flatMap(getEdgeIds)

  // Squash all actions on each edge to one single action
  val edgesWithAction = edgeIds.map(edgeIDs => (edgeIDs, logs.filter(log => log.objectType match {
    case VERTEX(objId) => (edgeIDs._1 == objId || edgeIDs._2 == objId) && log.action == DELETE // Vertex DELETES means that the edge is deleted as well
    case EDGE(srcId, dstId) => edgeIDs._1 == srcId && edgeIDs._2 == dstId
  })))
    .map(edgeWithAction => (edgeWithAction._1, mergeLogTSVs(edgeWithAction._2)))
  edgesWithAction
}

def applyVertexLogsToSnapshot(snapshot: Graph[LTSV.Attributes, LTSV.Attributes], logs: RDD[LogTSV]): RDD[(VertexId, Attributes)] = {
  val previousSnapshotVertices = snapshot.vertices.map(vertex => (vertex._1, vertex._2))

  val vertexIDsWithAction: RDD[(Long, LogTSV)] = getSquashedActionsByVertexId(logs)

  // Combine vertices from the snapshot with the current log interval
  val joinedVertices = previousSnapshotVertices.fullOuterJoin(vertexIDsWithAction)

  // Create, update or omit (delete) vertices based on values are present or not.
  val newSnapshotVertices = joinedVertices.flatMap(outerJoinedVertex => outerJoinedVertex._2 match {
    case (Some(snapshotVertex), None) => Some((outerJoinedVertex._1, snapshotVertex)) // No changes to the vertex in this interval
    case (Some(snapshotVertex), Some(newVertex)) => newVertex match {                 // Changes to the vertex in this interval
      case newVertex.action == DELETE => None                                         // Vertex was deleted
      case newVertex.action == UPDATE =>                                              // Vertex was updated
        Some((outerJoinedVertex._1, rightWayMergeHashMap(snapshotVertex, newVertex.attributes)))
    }
    case (None, Some(newVertex)) => newVertex match {                                 // Vertex was introduced in this interval
      case newVertex.action == DELETE => None                                         // Vertex was introduced then promptly deleted
      case newVertex.action == CREATE =>                                              // Vertex was created
        Some((outerJoinedVertex._1, newVertex.attributes))
      case newVertex.action == UPDATE =>                                              // Vertex was created and had an update
        Some((outerJoinedVertex._1, newVertex.attributes))
    }
  })
  newSnapshotVertices
}

def getSquashedActionsByVertexId(logs: RDD[LogTSV]): RDD[(Long, LogTSV)] = {
  // Get all ids of vertices in the log interval
  val vertexIds = logs.flatMap(getVertexIds)

  // Squash all actions on each vertex to one single action
  val verticesWithAction = vertexIds.map(id => (id, logs.filter(log => log.objectType match {
    case VERTEX(objId) => id == objId
    case EDGE(_, _) => false
  })))
    .map(vertexWithActions => (vertexWithActions._1, mergeLogTSVs(vertexWithActions._2)))
  verticesWithAction
}

def mergeLogTSVs(logs:RDD[LogTSV]):LogTSV = logs.reduce(mergeLogTSV)

 def mergeLogTSV(l1: LogTSV, l2: LogTSV) : LogTSV= {
   (l1.action, l2.action) match {
     case (UPDATE, DELETE) => l2 // DELETE nullifies previous operations
     case (CREATE, DELETE) => l2 // DELETE nullifies previous operations
     case (UPDATE, UPDATE) => l2.copy(attributes=rightWayMergeHashMap(l1.attributes, l2.attributes)) // Could be either l1 or l2 that is copied
     case (CREATE, UPDATE) => l1.copy(attributes=rightWayMergeHashMap(l2.attributes, l1.attributes))
     case (_,_) => assert(1==2); l1; // Cases that should not happen. We assume the LogTSV are consistent and makes sense
                                     // example (DELETE, DELETE), (DELETE, UPDATE) // These do not make sense
   }
 }
/** Merge two hashmaps with preference for dominantAttributes

 * Attributes is type alias for HashMap[(String,String)]
 * @param attributes recessive HashMap
 * @param dominantAttributes dominant HashMap
 * @return merged HashMap
 */
def rightWayMergeHashMap(attributes: LTSV.Attributes, dominantAttributes: LTSV.Attributes): LTSV.Attributes = {
    attributes.merged(dominantAttributes)((_, y) => y)
  }

// Could maybe be RDD[LogTSV] -> RDD[Long] instead
// But I do like seeing the transformations/actions where it's used
// so the Spark abstraction is not lost
// For example this function is just a transformation
// From the caller it could look like an action if it was used like
// edgeIds = getEdgeIds(logs)
// (The types would of course prove the assumption wrong, but it's nice
// either way
def getEdgeIds(log: LogTSV): Option[(Long,Long)] = {
  log.objectType match {
    case VERTEX(_) => None
    case EDGE(srcId, dstId) => Some(srcId,dstId)
  }
}

def getVertexIds(log: LogTSV): Option[Long] = {
  log.objectType match {
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
    val firstSnapshotVertices = getInitialVerticesFromLogs(logs)
    val firstSnapshotEdges = getInitialEdgesFromLogs(logs)

      Graph(firstSnapshotVertices, firstSnapshotEdges)
  }

  def getInitialVerticesFromLogs(logs: RDD[LogTSV]): RDD[(VertexId, Attributes)] = {
    val verticesWithAction = getSquashedActionsByVertexId(logs)
    verticesWithAction.flatMap(vertexTuple => vertexTuple._2.action match {
      case Action.CREATE => Some(vertexTuple._1, vertexTuple._2.attributes) // Vertex was created
      case Action.DELETE => None                                            // Vertex was created and deleted
      case Action.UPDATE => Some(vertexTuple._1, vertexTuple._2.attributes) // Vertex was created and updated
    })
  }

  def getInitialEdgesFromLogs(logs: RDD[LogTSV]): RDD[Edge[Attributes]] = {
    val edgesWithAction = getSquashedActionsByEdgeId(logs)
    edgesWithAction.flatMap(edge => edge._2.action match {
      case Action.CREATE => Some(Edge(edge._1._1, edge._1._2, edge._2.attributes)) // Edge was created
      case Action.DELETE => None                                                   // Edge was created and deleted (Possibly because of a deleted vertex)
      case Action.UPDATE => Some(Edge(edge._1._1, edge._1._2, edge._2.attributes)) // Edge was created and updated
    })
  }
