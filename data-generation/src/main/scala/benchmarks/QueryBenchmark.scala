package benchmarks

import org.apache.spark.SparkContext
import thesis.SparkConfiguration.getSparkSession
import utils.{Benchmark, FileWriter}

abstract class QueryBenchmark(val iterationCount: Int, val customColumn: String, benchmarkSuffixes: Seq[String] = Seq()) extends Serializable {
  implicit val sc: SparkContext = getSparkSession.sparkContext
  var benchmarks: Seq[Benchmark] = Seq()

  def run: Unit = {
    benchmarkSuffixes.length match {
      case 0 => initialize()
      case 1 => initialize()
      case _ => initialize(benchmarkSuffixes)
    }
    for (i <- 1 to iterationCount) {
      execute(i)
    }
    tearDown()
  }

  def initialize(): Unit = {
    val fileWriter = FileWriter(filename = getClass.getSimpleName)
    benchmarks = benchmarks :+ Benchmark(fileWriter, textPrefix = getClass.getSimpleName, customColumn = customColumn)
  }

  def initialize(benchmarkSuffixes: Seq[String]): Unit = {
    benchmarkSuffixes.foreach(suffix => {
      val benchmarkId = s"${getClass.getSimpleName}-$suffix"
      val fileWriter = FileWriter(filename = benchmarkId)
      benchmarks = benchmarks :+ Benchmark(fileWriter, textPrefix = benchmarkId, customColumn = customColumn)
    })
  }

  def execute(iteration: Int): Unit

  def tearDown(): Unit = benchmarks.foreach(_.close())
}