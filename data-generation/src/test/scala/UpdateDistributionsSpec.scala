import org.apache.spark.graphx.VertexRDD
import org.scalatest.flatspec.AnyFlatSpec
import thesis.CorrelationMode
import thesis.UpdateDistributions.sortVerticesByMode
import wrappers.SparkTestWrapper

class UpdateDistributionsSpec extends AnyFlatSpec with SparkTestWrapper {


  // Really hard to test as almost every function in this Object are nondeterministic with all the randomnes

  "thesis.sortVerticesByMode" should "sort correctly with Uniform mode" in {
    val sc = spark.sparkContext
    val originalList = List((1L, 1), (2L, 2), (3L, 3))
    val vertices: VertexRDD[Int] = VertexRDD(sc.parallelize(originalList))
    val mode = CorrelationMode.Uniform
    val newList: List[(Long, Int)] = sortVerticesByMode(vertices, mode).collect().toList
    assert(originalList != newList) // How do you test if a list is sorted randomly?
  }
  it should "sort correctly with PositiveCorrelation" in {
    val sc = spark.sparkContext
    val originalList = List((1L, 1), (2L, 2), (3L, 3))
    val vertices: VertexRDD[Int] = VertexRDD(sc.parallelize(originalList))
    val mode = CorrelationMode.PositiveCorrelation
    val newList: List[(Long, Int)] = sortVerticesByMode(vertices, mode).collect().toList
    assert(originalList == newList)
  }

  it should "sort correctly with NegativeCorrelation" in {
    val sc = spark.sparkContext
    val originalList = List((1L, 1), (2L, 2), (3L, 3))
    val vertices: VertexRDD[Int] = VertexRDD(sc.parallelize(originalList))
    val mode = CorrelationMode.NegativeCorrelation
    val newList: List[(Long, Int)] = sortVerticesByMode(vertices, mode).collect().toList
    assert(originalList.reverse == newList)
  }

}
