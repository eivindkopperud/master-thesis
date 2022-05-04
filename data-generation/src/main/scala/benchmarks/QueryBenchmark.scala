package benchmarks

import org.apache.spark.SparkContext
import thesis.SparkConfiguration.getSparkSession
import utils.{Benchmark, FileWriter}

abstract class QueryBenchmark(val iterationCount: Int, val customColumn: String) extends Serializable {
  implicit val sc: SparkContext = getSparkSession.sparkContext
  var benchmark: Benchmark = null

  def run: Unit = {
    initialize()
    for (i <- 1 to iterationCount) {
      execute(i)
    }
    tearDown()
  }

  def initialize(): Unit = {
    val fileWriter = FileWriter(filename = getClass.getSimpleName)
    benchmark = Benchmark(fileWriter, textPrefix = getClass.getSimpleName, customColumn = customColumn)
  }

  def execute(iteration: Int): Unit

  def tearDown(): Unit = benchmark.close()
}