package benchmarks

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

  val distribution: Int => DistributionType = (iteration: Int) => distributionType match {
    case LogNormalType(param1, param2) => LogNormalType(param1, iteration * param2)
    case GaussianType(param1, param2) => GaussianType(iteration * param1, param2)
    case UniformType(param1, param2) => UniformType(param1, iteration * param2)
    case ZipfType(param1, param2) => ZipfType(param1, iteration * param2)
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
