package factories

import com.github.javafaker.Faker
import org.apache.spark.graphx.VertexId
import thesis.Action.{CREATE, DELETE, UPDATE}
import thesis.Entity.{EDGE, VERTEX}
import thesis.LTSV.Attributes
import thesis.{Action, LogTSV}
import utils.TimeUtils

import java.time.Instant
import scala.collection.immutable.HashMap
import scala.util.Random
import scala.util.Random.nextInt

case class LogFactory(
                       vertexIds: Seq[Int] = Seq(1, 2, 3, 4),
                       startTime: Instant = Instant.parse("2020-01-01T00:00:00.000Z"),
                       endTime: Instant = Instant.parse("2021-01-01T00:00:00.000Z"),
                     ) {
  /* TODO Merge functions here to create a standardized way of generating logs */
  def getOne: LogTSV = {
    val actions = List(CREATE, UPDATE, DELETE)
    LogTSV(
      timestamp = TimeUtils.getRandomOrderedTimestamps(1, startTime, endTime).head,
      action = actions(nextInt(actions.size)),
      entity = VERTEX(vertexIds(nextInt(vertexIds.size))),
      attributes = getRandomAttributes
    )
  }

  def buildSingleSequence(updateAmount: Int = 5, id: Long): Seq[LogTSV] = {
    val timestamps = TimeUtils.getRandomOrderedTimestamps(updateAmount, startTime, endTime)
    val create = Seq(LogTSV(
      timestamp = timestamps.head,
      action = CREATE,
      entity = VERTEX(id),
      attributes = getRandomAttributes
    )
    )
    val updates = timestamps.tail.map(timestamp => LogTSV(
      timestamp = timestamp,
      action = UPDATE,
      entity = VERTEX(id),
      attributes = getRandomAttributes
    )
    )
    create ++ updates
  }

  def buildSingleSequenceOnlyUpdates(updateAmount: Int = 5, id: Long): Seq[LogTSV] = {
    buildSingleSequence(updateAmount + 1, id).tail
  }

  def getCreateDeleteEdgeTSV(srcAndDstId: (Long, Long), timestamp: Instant, action: Action): LogTSV = {
    assert(action == CREATE || action == DELETE) // Im lazy
    LogTSV(
      timestamp = timestamp,
      action = action,
      entity = EDGE.tupled(srcAndDstId),
      attributes = if (action == CREATE) getRandomAttributes else HashMap.empty
    )
  }

  def generateEdgeTSV(srcIdAndDstId: (Long, Long), timestamp: Long): LogTSV = {
    LogTSV(
      timestamp = Instant.ofEpochSecond(timestamp),
      action = UPDATE,
      entity = EDGE.tupled(srcIdAndDstId),
      attributes = getRandomAttributes
    )
  }

  def generateVertexTSV(id: VertexId, timestamp: Long): LogTSV = {
    LogTSV(
      timestamp = Instant.ofEpochSecond(timestamp),
      action = UPDATE,
      entity = VERTEX(id),
      attributes = getRandomAttributes
    )
  }

  def getRandomAttributes: Attributes = {
    val faker = new Faker()
    HashMap[String, String](
      ("color", faker.color().name()),
      ("animal", faker.animal().name()),
      ("size", new Random().nextInt(10000).toString)
    )
  }
}
