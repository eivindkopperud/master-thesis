import factories.LogFactory
import org.apache.spark.SparkContext
import org.scalatest.flatspec.AnyFlatSpec
import thesis.{EDGE, Landy, VERTEX}
import wrappers.SparkTestWrapper

class LandyConsumerSpec extends AnyFlatSpec with SparkTestWrapper {
  "LandyConsumer" can "consume vertices" in {
    implicit val sc: SparkContext = spark.sparkContext
    val logs = LogFactory().buildSingleSequence(VERTEX(10))
    val logsRDD = sc.parallelize(logs)
    val landy = Landy(logsRDD)

    assert(landy.underlyingGraph.vertices.count() == 5)
    assert(landy.underlyingGraph.edges.count() == 0)
  }

  it can "consume edges" in {
    implicit val sc: SparkContext = spark.sparkContext
    val logs = LogFactory().buildSingleSequence(EDGE(1, 10, 11))
    val logsRDD = sc.parallelize(logs)
    val landy = Landy(logsRDD)

    assert(landy.underlyingGraph.vertices.count() == 2)
    assert(landy.underlyingGraph.edges.count() == 5)


  }
}
