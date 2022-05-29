package thesis

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import thesis.DataTypes.{Attributes, EdgeId}

import java.time.Instant

trait TemporalGraph {
  def snapshotAtTime(instant: Instant): Snapshot

  def directNeighbours(vertexId: VertexId, interval: Interval): RDD[VertexId]

  def getEntity[T <: Entity](entity: T, timestamp: Instant): Option[(T, Attributes)]

  def activatedEntities(interval: Interval): (RDD[VertexId], RDD[EdgeId])
}
