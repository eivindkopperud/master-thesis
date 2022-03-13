import Action.{CREATE, DELETE, UPDATE}
import Entity.{EDGE, VERTEX}
import LTSV.{Attributes, deserializeLTSV}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.reflect.ClassTag

class SnapshotDelta[VD: ClassTag, ED: ClassTag](val graphs: List[Graph[Attributes, Attributes]], val logs:RDD[LogTSV]) extends Graph[VD, ED] {
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

object SnapshotDelta {
  // Delete me
  def apply[VD: ClassTag, ED: ClassTag](
                                         vertices: RDD[(VertexId, VD)],
                                         edges: RDD[Edge[ED]],
                                         defaultVertexAttr: VD = null.asInstanceOf[VD],
                                         edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                         vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {
    SnapshotDelta(vertices, edges)
  }

  def create[VD,ED](logs:RDD[LogTSV], snapType: SnapshotIntervalType):SnapshotDelta[VD,ED] = {
    snapType match {
      case SnapshotIntervalType.Time(duration) => snapShotTime(logs, duration)
      case SnapshotIntervalType.Count(numberOfActions) => snapShotCount(logs, numberOfActions)
    }
  }

  def snapShotCount(logs: RDD[LogTSV], numberOfActions: Int): SnapshotDelta[Attributes,Attributes] = {
    val initialGraph = createGraph(logs.map(log => (log.sequentialId, log)).filterByRange(0, numberOfActions).map(_._2))
    val graphs = mutable.MutableList(initialGraph)
      for ( i <- 1 to (logs.count() / numberOfActions).ceil.toInt) {
        // Get subset of logs
        val previousSnapshot = graphs(i - 1)
        val logInterval = logs
          .map(log => (log.sequentialId, log))                                  // Mapping like this makes it into a KeyValuePairRDD, which have some convenient methods
          .filterByRange(numberOfActions * i, numberOfActions * (i + 1)) // This lets ut get a slice of the LTSVs
          .map(_._2) // This maps the type into back to its original form

        // Get all ids of vertices in the log interval
        val vertexIds = logInterval.map( log => {
          log.objectType match {
            case VERTEX(objId) => Some(objId)
            case EDGE(_, _) => None
          }
        }).filter(_.isDefined).map(_.get)

        // Squash all actions on each vertex to one single action
        val verticesWithAction = vertexIds.map(id => (id, logInterval.filter( log => log.objectType match {
          case VERTEX(objId) => id == objId
          case EDGE(_, _) => false
        })))
          .map(vertexWithActions => (vertexWithActions._1, mergeLogTSVs(vertexWithActions._2)))

        // Get all ids of edges in the log interval
        val edgeIds = logInterval.map( log => {
          log.objectType match {
            case VERTEX(_) => None
            case EDGE(srcId, dstId) => Some(srcId, dstId)
          }
        }).filter(_.isDefined).map(_.get)

        // Squash all actions on each edge to one single action
        val edgesWithAction = edgeIds.map( id => (id, logInterval.filter( log => log.objectType match {
          case VERTEX(objId) => id == objId
          case EDGE(srcId, dstId) => id == srcId || id == dstId
        })))
          .map(edgeWithAction => (edgeWithAction._1, mergeLogTSVs(edgeWithAction._2)))
          .map(edge => Edge(edge._1._1, edge._1._2, edge._2))

        // We now need to get on the format
        // (previousSnapshotEdge, newSnapshotAction)
        // such that we map the previous state to the new state by merging attributes
        // (this does also hold for vertices)

        // We also need to add new edges to the edgeRDD when CREATE appears

        // val updatedVertices = ..old vertices with new merged action applied..
        // val updatedEdges = ..old edges with new merged action applied..
        graphs += Graph(updatedVertices, updatedEdges)
      })

        Graph(vertices, edges)
      } )
  }
  def mergeLogTSVs(logs:RDD[LogTSV]):LogTSV = logs.reduce(mergeLogTSV)

 def mergeLogTSV(l1: LogTSV, l2: LogTSV) : LogTSV= {
   (l1.action, l2.action) match {
     case (UPDATE, DELETE) => l2
     case (CREATE, DELETE) => l2
     case (UPDATE, UPDATE) => l2.copy(attributes=rightWayMergeHashMap(l1.attributes, l2.attributes))
     case (CREATE, UPDATE) => l1.copy(attributes=rightWayMergeHashMap(l2.attributes, l1.attributes))
     case (_,_) => assert(1==2); l1;

   }
 }
// Attributes: HashMap(String,String)
def rightWayMergeHashMap(attributes: LTSV.Attributes, attributes1: LTSV.Attributes):LTSV.Attributes = {
    attributes.merged(attributes1)((_,y) => y)
  }

  def createGraph(logs:RDD[LogTSV]):Graph[Attributes,Attributes] = {
    // Might need to be re-implemented
    // vertices = ...
    // edges = ...

    Graph(vertices, edges)
  }
}
