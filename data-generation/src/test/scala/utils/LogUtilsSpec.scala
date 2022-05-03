package utils

import factories.LogFactory
import org.scalatest.flatspec.AnyFlatSpec
import thesis.Entity.{EDGE, VERTEX}
import wrappers.SparkTestWrapper

class LogUtilsSpec extends AnyFlatSpec with SparkTestWrapper {
  behavior of "LogUtils"
  it should "group vertex logs by id" in {
    val updateAmount = 5
    val seq1 = LogFactory().buildSingleSequence(VERTEX(1L), updateAmount)
    val seq2 = LogFactory().buildSingleSequence(VERTEX(2L), updateAmount)
    val logsRDD = spark.sparkContext.parallelize(seq1 ++ seq2)
    val vertexLogs = LogUtils.groupVertexLogsById(logsRDD)

    assert(vertexLogs.count() == 2)
    assert(vertexLogs.take(1)(0)._2.size == updateAmount)
  }

  it should "group edge logs by id" in {
    val updateAmount = 5
    val seq1 = LogFactory().buildSingleSequence(EDGE(1L, 10L, 11L), updateAmount)
    val seq2 = LogFactory().buildSingleSequence(EDGE(2L, 10L, 11L), updateAmount)
    val logsRDD = spark.sparkContext.parallelize(seq1 ++ seq2)
    val edgeLogs = LogUtils.groupEdgeLogsById(logsRDD)

    assert(edgeLogs.count() == 2)
    assert(edgeLogs.take(1)(0)._2.size == updateAmount)
  }

  it should "group edges by surrogate key" in {
    val updateAmount = 5
    val seq1 = LogFactory().buildSingleSequence(EDGE(1L, 2L, 3L), updateAmount)
    val seq2 = LogFactory().buildSingleSequence(EDGE(1L, 4L, 5L), updateAmount)
    val logsRDD = spark.sparkContext.parallelize(seq1 ++ seq2)
    val edgeLogs = LogUtils.groupEdgeLogsById(logsRDD)

    assert(edgeLogs.count() == 1)
    assert(edgeLogs.take(1)(0)._2.size == 2 * updateAmount)
  }

  it should "reverse logs" in {
    val logs = LogFactory().buildSingleSequence(VERTEX(1L))
    val reversedLogs = LogUtils.reverse(logs)

    assert(logs.head.equals(reversedLogs.last))
    assert(reversedLogs.head.equals(logs.last))
  }
}
