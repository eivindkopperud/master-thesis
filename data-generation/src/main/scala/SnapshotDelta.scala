import Action.{DELETE, UPDATE}
import Entity.{EDGE, VERTEX}
import LTSV.Attributes
import org.apache.commons.lang.mutable.Mutable
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

  def snapShotCount(logs:RDD[LogTSV], numberOfActions:Int): SnapshotDelta[Attributes,Attributes] ={
    val initalGraph = createGraph(logs.map(x => (x.sequentialId,x)).filterByRange(0,numberOfActions).map(_._2))
    var graphs = mutable.MutableList(initalGraph)
      for ( i <- 1 to (logs.count()/numberOfActions).ceil.toInt) {
        // Get subset of logs
        val previousSnapshot = graphs(i-1)
        val LTSVInterval = logs
          .map(x => (x.sequentialId, x))                                  // Mapping like this makes it into a KeyValuePairRDD, which have some convenient methods
          .filterByRange(numberOfActions * i, numberOfActions * (i + 1)) // This lets ut get a slice of the LTSVs
          .map(_._2) // This maps the type into back to its original form

        // Lets get all the relevant LTSV for each Edge (Edge,RDD[LTSV])
        val edges =
          previousSnapshot.edges
            .map(e => (e, LTSVInterval.filter(z => z.objectType match {
              case VERTEX(objId) => (objId == e.dstId || objId == e.srcId) && z.action == DELETE // DELETE vertex operations is relevant for edges since it will delete them as well
              case EDGE(srcId, dstId) => srcId == e.srcId && dstId == e.dstId
            }
            )))

        // Lets get all the relevant LTSV for each Edge (Vertex,RDD[LTSV])
        // This can be possibly rewritten into groupByKey
        //}
        val vertices = previousSnapshot.vertices.map(v => (v, LTSVInterval.filter(z => z.objectType match {
          case VERTEX(objId) => v._1 == objId
          case EDGE(_, _) => false
        })))

        //MISSING CREATE UPDATE DELETE FOR NEW VERTICES AND EDGES

        val processed_edges = edges.map( x => {
          val (edge, logs) = x
          val bigLTSV = logs.reduce(mergeLogTSV)
          bigLTSV.action match {
            case UPDATE => Some(Edge(edge.srcId, edge.dstId, rightWayMergeHashMap(edge.attr, bigLTSV.attributes)))
            case DELETE => None
          }
        }).filter(_.isDefined).map(_.get)

        val processed_vertices = vertices.map(x => {
          val ((id, attr),logs) = x
          val newLTSV = mergeLogTSVs(logs)
          newLTSV.action match {
            case UPDATE => Some((id, rightWayMergeHashMap(attr, newLTSV.attributes)))
            case DELETE => None
          }
        }).filter(_.isDefined).map(_.get)
        graphs += Graph(processed_vertices, processed_edges)
      })

        Graph(vertices, edges)
      } )
  }
  def mergeLogTSVs(logs:RDD[LogTSV]):LogTSV = logs.reduce(mergeLogTSV)

 def mergeLogTSV(l1: LogTSV, l2: LogTSV) : LogTSV= {
   (l1.action, l2.action) match {
     case (UPDATE, DELETE) => l2
     case (UPDATE, UPDATE) => l2.copy(attributes=rightWayMergeHashMap(l1.attributes, l2.attributes))
     case (_,_) => assert(1==2); l1;

   }
 }
// Attributes: HashMap(String,String)
def rightWayMergeHashMap(attributes: LTSV.Attributes, attributes1: LTSV.Attributes):LTSV.Attributes = {
    attributes.merged(attributes1)((_,y) => y)
  }

// This is wrong
  def createGraph(logs:RDD[LogTSV]):Graph[Attributes,Attributes] = {
    val vertexIds = logs.map( ltsv => {
      ltsv.objectType match {
        case VERTEX(objId) => Some(objId)
        case EDGE(srcId, dstId) => None
      }
    }).filter(_.isDefined).map(_.get)

    //should be identical
    val vertexPlusTSVs = vertexIds.map( id => (id, logs.filter( ltsv => ltsv.objectType match {
      case VERTEX(objId) => id == objId
      case EDGE(_, _) => false
    })))

    //should be identical
    val vertexIds2 = logs.map(ltsv => ltsv.objectType match {
      case VERTEX(objId) => (Some(objId, ltsv))
      case EDGE(srcId, dstId) => None
    }).filter(_.isDefined).map(_.get).groupByKey()

    val edgePlusTSVs = logs.map(ltsv => ltsv.objectType match {
      case VERTEX(objId) => None
      case EDGE(srcId, dstId) => (Some((srcId, dstId), ltsv))
    }).filter(_.isDefined).map(_.get).groupByKey()

    // In the end handle get all deletes and delete stuff


    Graph(vertices, edges)
  }
}
