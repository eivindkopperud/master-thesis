package thesis

import org.apache.spark.graphx.{EdgeRDD, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import thesis.DataTypes.EdgeId

import java.time.Instant
import scala.reflect.ClassTag

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
