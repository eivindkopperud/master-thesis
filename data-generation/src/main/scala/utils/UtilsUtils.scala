package utils

import org.apache.spark.graphx.{Edge, VertexId}
import thesis.DataTypes.{AttributeGraph, Attributes}
import thesis.DistributionType.{GaussianType, LogNormalType, UniformType, ZipfType}
import thesis.{DataSource, DistributionType, SnapshotEdgePayload}

object UtilsUtils {
  def uuid: Long = java.util.UUID.randomUUID.getLeastSignificantBits & Long.MaxValue

  def zipGraphs(g1: AttributeGraph, g2: AttributeGraph):
  (Seq[((VertexId, Attributes), (VertexId, Attributes))], Seq[(Edge[SnapshotEdgePayload], Edge[SnapshotEdgePayload])]) =
    (g1.vertices.zip(g2.vertices).collect(), g1.edges.zip(g2.edges).collect())

  def getConfig(key: String): String = sys.env(key)

  def getConfigSafe(key: String): Option[String] = sys.env.get(key)

  def loadThreshold(): Int = UtilsUtils.getConfigSafe("THRESHOLD") match {
    case Some(value) => value.toInt
    case None => 40
  }

  /**
   * Try to load data source. Return ContactsHyperText as default.
   */
  def loadDataSource(): DataSource = UtilsUtils.getConfigSafe("DATA_SOURCE") match {
    case Some(value) =>
      if (value.toInt == 1) return DataSource.Reptilian
      if (value.toInt == 2) return DataSource.ContactsHyperText
      if (value.toInt == 3) return DataSource.FbMessages
      DataSource.Reptilian
    case None => DataSource.ContactsHyperText
  }

  def loadTimestamp(): Long = UtilsUtils.getConfigSafe("TIMESTAMP") match {
    case Some(value) => value.toLong
    case None => 0
  }

  def loadIntervalDelta(): Int = UtilsUtils.getConfigSafe("INTERVAL_DELTA") match {
    case Some(value) => value.toInt
    case None => 0
  }

  def loadVertexId(): Long = UtilsUtils.getConfigSafe("VERTEX_ID") match {
    case Some(value) => value.toLong
    case None => 0
  }

  def loadDistributionType(): DistributionType = {
    UtilsUtils.getConfigSafe("DISTRIBUTION_TYPE") match {
      case Some(value) =>
        if (value.toInt == 1) return UniformType(0, 0)
        if (value.toInt == 2) return LogNormalType(0, 0)
        if (value.toInt == 3) return GaussianType(0, 0)
        if (value.toInt == 4) return ZipfType(0, 0)
        UniformType(0, 0)
      case None => UniformType(0, 0)
    }
  }
}
