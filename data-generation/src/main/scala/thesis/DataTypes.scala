package thesis

import org.apache.spark.graphx.Graph
import thesis.DataTypes.{AttributeGraph, Attributes, EdgeId}

import java.time.Instant
import scala.collection.immutable

object DataTypes {
  //SnapshotDeltaGraph
  type AttributeGraph = Graph[Attributes, SnapshotEdgePayload]
  //LandyGraph
  type LandyAttributeGraph = Graph[LandyVertexPayload, LandyEdgePayload]
  // Type alias
  type Attributes = immutable.HashMap[String, String]
  // Like VertexId but for Edges since we are in dire need of a surrogate
  type EdgeId = Long
}

case class Interval(start: Instant, stop: Instant)

case class SnapshotEdgePayload(id: EdgeId, attributes: Attributes)


sealed abstract class SnapshotIntervalType

object SnapshotIntervalType {
  final case class Time(duration: Int) extends SnapshotIntervalType

  final case class Count(numberOfActions: Int) extends SnapshotIntervalType
}

final case class Snapshot(graph: AttributeGraph, instant: Instant)

case class LandyVertexPayload(id: Long, validFrom: Instant, validTo: Instant, attributes: Attributes)

case class LandyEdgePayload(id: Long, validFrom: Instant, validTo: Instant, attributes: Attributes)

sealed abstract class DataSource

object DataSource {
  final case object Reptilian extends DataSource

  final case object FbMessages extends DataSource

}

sealed abstract class CorrelationMode

object CorrelationMode {
  final case object Uniform extends CorrelationMode

  final case object PositiveCorrelation extends CorrelationMode

  final case object NegativeCorrelation extends CorrelationMode
}

sealed abstract class DistributionType

object DistributionType {
  final case class LogNormalType(mu: Int, sigma: Double) extends DistributionType

  final case class GaussianType(mu: Int, sigma: Double) extends DistributionType

  final case class UniformType(low: Double, high: Double) extends DistributionType
}

final case class IntervalAndUpdateCount(interval: Interval, count: Int)

final case class IntervalAndDegrees(interval: Interval, degree: Int)


/** Action Enum
 *
 */
sealed abstract class Action

object Action {
  final case object CREATE extends Action

  final case object UPDATE extends Action

  final case object DELETE extends Action
}

sealed abstract class Entity

object Entity {
  final case class VERTEX(objId: Long) extends Entity

  final case class EDGE(id: Long, srcId: Long, dstId: Long) extends Entity
}

/** LogTSV
 *
 * Everything is built around this case class
 *
 * @param timestamp  When did the log_entry happen?
 * @param action     Type of action
 * @param entity     The type of the object with id(s)
 * @param attributes List of (Key,Value) attributes relevant to the entry
 */
case class LogTSV(
                   timestamp: Instant,
                   action: Action,
                   entity: Entity,
                   attributes: Attributes
                 )

