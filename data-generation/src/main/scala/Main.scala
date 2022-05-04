import factories.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import thesis.SparkConfiguration.getSparkSession
import thesis.{Landy, VERTEX}
import utils.{Benchmark, FileWriter}


object Main extends App {
  val spark: SparkSession = getSparkSession
  implicit val sc: SparkContext = spark.sparkContext

  val b = Benchmark(FileWriter(filename = "snapshot-landy"), textPrefix = "Snapshot", customColumn = "number of logs")
  b.writeHeader
  for (i <- 1 to 5) {
    val numberOfLogs = scala.math.pow(2, i.toDouble).toInt
    val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numberOfLogs)
    val graph = Landy(sc.parallelize(logs))
    val timestamp = logs.head.timestamp
    b.benchmarkSingle(graph.snapshotAtTime(timestamp), customColumnValue = numberOfLogs.toString)
  }
  b.close() // should be called if its a FileWriter
}
