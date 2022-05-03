import factories.LandyGraphFactory.createGraph
import factories.{LandyGraphFactory, LogFactory}
import org.apache.spark.SparkContext
import org.scalatest.flatspec.AnyFlatSpec
import thesis.{EDGE, Interval, Landy, VERTEX}
import utils.TimeUtils
import wrappers.SparkTestWrapper

import java.time.Instant


class LandySpec extends AnyFlatSpec with SparkTestWrapper {

  "Landy" can "be created" in {
    val landy: Landy = new Landy(createGraph())

    assert(landy.vertices.count() == 5)
    assert(landy.edges.count() == 2)
  }

  it can "give a snapshot" in {
    val landy: Landy = new Landy(createGraph())

    val graph = landy.snapshotAtTime(t2)

    assert(graph.vertices.count() == 2)
    assert(graph.edges.count() == 1)
  }

  it can "query activated edges" in {
    implicit val sparkContext: SparkContext = spark.sparkContext
    val start = Instant.parse("2000-01-01T00:00:00Z")
    val end = Instant.parse("2000-01-01T10:00:00Z")
    val timestamps = TimeUtils.getDeterministicOrderedTimestamps(amount = 11, startTime = start, endTime = end)

    val vertexLogs1 = LogFactory(startTime = timestamps(2), endTime = timestamps(8)).buildSingleSequence(VERTEX(1))
    val vertexLogs2 = LogFactory(startTime = timestamps(5), endTime = timestamps(10)).buildSingleSequence(VERTEX(2))
    val edgeLogs = LogFactory(startTime = timestamps(4), endTime = timestamps(8)).buildSingleSequence(EDGE(1, 1, 2))
    val logs = (vertexLogs1 ++ vertexLogs2 ++ edgeLogs).sortBy(_.timestamp)
    val logRDD = sparkContext.parallelize(logs)
    val g = Landy(logRDD)

    val intervalWithVertex1 = g.activatedEntities(Interval(timestamps.head, timestamps(3)))
    val intervalWithVertex2 = g.activatedEntities(Interval(timestamps(5), timestamps(7)))
    val intervalWithEdge = g.activatedEntities(Interval(timestamps(3), timestamps(5)))
    val intervalWithV1andEdge = g.activatedEntities(Interval(timestamps.head, timestamps(4)))
    val intervalWithV2andEdge = g.activatedEntities(Interval(timestamps(4), timestamps(7)))
    val intervalWithAll = g.activatedEntities(Interval(timestamps.head, timestamps(10)))


    assert(intervalWithVertex1._2.isEmpty() && intervalWithVertex1._1.count() == 1)

    assert(intervalWithVertex2._1.count() == 1)

    assert(intervalWithEdge._2.count() == 1)
    assert(intervalWithV1andEdge._1.count() == 1, intervalWithV1andEdge._2.count() == 1)
    assert(intervalWithV2andEdge._1.count() == 1, intervalWithV2andEdge._2.count() == 1)

    assert(intervalWithAll._1.count() == 2 && intervalWithV1andEdge._2.count() == 1)
  }
}
