package benchmarks

import breeze.stats.distributions.{Gaussian, LogNormal, Uniform}
import org.apache.spark.graphx.Graph
import thesis.DistributionType.{GaussianType, LogNormalType, UniformType, ZipfType}
import thesis.TopologyGraphGenerator.generateGraph
import thesis.{DistributionType, Interval}
import utils.UtilsUtils

class ComparisonBenchmark(distributionType: DistributionType,
                          iterationCount: Int = 5,
                          customColumn: String = "Number of logs",
                          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
                         ) extends QueryBenchmark(iterationCount, customColumn, benchmarkSuffixes) {
  val threshold = UtilsUtils.loadThreshold()
  val dataSource = UtilsUtils.loadDataSource()
  val param1 = UtilsUtils.getConfig("DISTRIBUTION_PARAM1").toDouble
  val param2 = UtilsUtils.getConfig("DISTRIBUTION_PARAM2").toDouble

  val distribution = (iteration: Int) => distributionType match {
    case _: LogNormalType => LogNormalType(param1.toInt, iteration * param2)
    case _: GaussianType => GaussianType(iteration * param1.toInt, param2)
    case _: UniformType => UniformType(param1, iteration * param2)
    case _: ZipfType => ZipfType(param1.toInt, iteration * param2)
  }

  def getMean(iteration: Int): Double = distributionType match {
    case _: LogNormalType => LogNormal(param1, iteration * param2).mean
    case _: GaussianType => Gaussian(param1.toInt, iteration * param2).mean
    case _: UniformType => Uniform(param1, iteration * param2).mean
    case _ => 0
  }

  val vertexId = UtilsUtils.loadVertexId()

  lazy val graph: Graph[Long, Interval] = {
    generateGraph(threshold, dataSource).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
    })
  }

  override def execute(iteration: Int): Unit = {}
}
