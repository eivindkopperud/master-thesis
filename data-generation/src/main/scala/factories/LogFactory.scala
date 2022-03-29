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

  /** Fine grained control on number of updates and when they happen
   *
   * eg. [5,0,3]
   * Will give 5 updates at t_1, 0 at t_2 and 3 at t_3
   *
   * @param numberOfUpdates list of updates with index in list matching timestamp
   * @param id              Just some id for the vertex
   * @return
   */
  def buildIrregularSequence(numberOfUpdates: List[Int], id: Long = 1): Seq[LogTSV] = {
    val create = Seq(LogTSV(
      timestamp = Instant.ofEpochSecond(0),
      action = CREATE,
      entity = VERTEX(id),
      attributes = getRandomAttributes
    ))
    val updates = numberOfUpdates.zipWithIndex.tail.flatMap(x => {
      val (numUpdates, timestamp) = x
      Seq.range(0, numUpdates).map(
        _ =>
          LogTSV(
            timestamp = Instant.ofEpochSecond(timestamp),
            action = UPDATE,
            entity = VERTEX(id),
            attributes = getRandomAttributes
          )
      )
    }
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

  def generateVertexTSV(id: VertexId, timestamp: Long, attributes: Attributes = getRandomAttributes): LogTSV = {
    LogTSV(
      timestamp = Instant.ofEpochSecond(timestamp),
      action = UPDATE,
      entity = VERTEX(id),
      attributes = attributes
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
