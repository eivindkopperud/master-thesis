import factories.LogFactory
import org.apache.spark.SparkContext
import org.scalatest.flatspec.AnyFlatSpec
import thesis.Entity.{EDGE, VERTEX}
import thesis.LandyConsumer
import wrappers.SparkTestWrapper

class LandyConsumerSpec extends AnyFlatSpec with SparkTestWrapper {
  "LandyConsumer" can "consume vertices" in {
    implicit val sc: SparkContext = spark.sparkContext
    val logs = LogFactory().buildSingleSequence(VERTEX(10))
    val logsRDD = sc.parallelize(logs)
    val graph = LandyConsumer.consume(logsRDD)

    assert(graph.vertices.count() == 5)
    assert(graph.edges.count() == 0)
  }

  it can "consume edges" in {
    implicit val sc: SparkContext = spark.sparkContext
    val logs = LogFactory().buildSingleSequence(EDGE(10, 11))
    val logsRDD = sc.parallelize(logs)
    val graph = LandyConsumer.consume(logsRDD)

    assert(graph.vertices.count() == 2)
    assert(graph.edges.count() == 5)


  }
}
