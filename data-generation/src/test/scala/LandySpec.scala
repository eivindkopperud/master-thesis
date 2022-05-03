import factories.LandyGraphFactory.createGraph
import thesis.{Interval, Landy}
import org.scalatest.flatspec.AnyFlatSpec
import factories.LogFactory
import org.apache.spark.SparkContext
import thesis.{EDGE, VERTEX}
import utils.TimeUtils
import utils.TimeUtils.{t1, t2, t3, t4}
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

  it can "query direct neighbours" in {
    implicit val sparkContext: SparkContext = spark.sparkContext
    val g = new Landy(createGraph())

    // Assertions for 1L's neighbours through time
    assert(g.directNeighbours(1L, Interval(t3, t3)).collect().toSeq == Seq(2L))
    assert(g.directNeighbours(1L, Interval(t4, t4)).collect().toSeq == Seq())

    // Assertions for 2L's neighbours through time
    assert(g.directNeighbours(2L, Interval(t3, t3)).collect().toSeq == Seq(1L))
    assert(g.directNeighbours(2L, Interval(t4, t4)).collect().toSeq == Seq())

  it can "query for vertex edges" in {
    val graph = new Landy(createGraph())

    assert(graph.getEntity(VERTEX(1L), t1).get._2("color") == "red")
    // assert(graph.getEntity(VERTEX(1L), t2).get._2("color") == "orange") currently fails due to overlapping values in t2
    assert(graph.getEntity(VERTEX(1L), t3).get._2("color") == "orange")
    assert(graph.getEntity(VERTEX(1L), t4).isEmpty)

    assert(graph.getEdge(EDGE(1003L, 1L, 2L), t1).get._2("relation") == "brother")
    // assert(graph.getEdge(EDGE(1003L, 1L, 2L), t2).get._2("relation") == "sister") currently fails due to overlapping values in t2
    assert(graph.getEdge(EDGE(1003L, 1L, 2L), t3).get._2("relation") == "sister")
    assert(graph.getEdge(EDGE(1003, 1L, 2L), t4).isEmpty)
  }
}
