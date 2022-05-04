import factories.LandyGraphFactory.createGraph
import factories.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import thesis.Entity.VERTEX
import thesis.SparkConfiguration.getSparkSession
import thesis.{Landy, PossibleWorkFlow}
import utils.TimeUtils.t1
import utils.{Benchmark, FileWriter, LogUtils, Writer}


object Main extends App {
  val spark: SparkSession = getSparkSession
  implicit val sc: SparkContext = spark.sparkContext

  val b = Benchmark(FileWriter(filename = "snapshot-landy"), textPrefix = "Snapshot", customColumn = "number of logs")
  b.writeHeader
  for (i <- 1 to 14) {
    val numberOfLogs = scala.math.pow(2, i.toDouble).toInt
    val logs = LogFactory().buildSingleSequenceWithDelete(VERTEX(1), updateAmount = numberOfLogs)
    val graph = Landy(sc.parallelize(logs))
    val timestamp = logs.head.timestamp
    b.benchmarkSingle(graph.snapshotAtTime(timestamp), customColumnValue = numberOfLogs.toString)
  }
  b.close() // should be called if its a FileWriter
}
