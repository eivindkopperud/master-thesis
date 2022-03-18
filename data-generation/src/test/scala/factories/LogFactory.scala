package factories

import com.github.javafaker.Faker
import thesis.Action.{CREATE, UPDATE, DELETE}
import thesis.Entity.VERTEX
import thesis.LogTSV
import utils.TimeUtils

import java.time.Instant
import scala.collection.immutable
import scala.util.Random.nextInt

case class LogFactory(
                       vertexIds: Seq[Int] = Seq(1, 2, 3, 4),
                       startTime: Instant = Instant.parse("2020-01-01T00:00:00.000Z"),
                       endTime: Instant = Instant.parse("2021-01-01T00:00:00.000Z"),
                     ) {
  def getOne: LogTSV = {
    val actions = List(CREATE, UPDATE, DELETE)
    LogTSV(
      timestamp = TimeUtils.getRandomOrderedTimestamps(1, startTime, endTime).head,
      action = actions(nextInt(actions.size)),
      entity = VERTEX(vertexIds(nextInt(vertexIds.size))),
      attributes = attributeFaker
    )
  }

  def buildSingleSequence(updateAmount: Int = 5, id: Long): Seq[LogTSV] = {
    val timestamps = TimeUtils.getRandomOrderedTimestamps(updateAmount, startTime, endTime)
    val create = Seq(LogTSV(
      timestamp = timestamps.head,
      action = CREATE,
      entity = VERTEX(id),
      attributes = attributeFaker
      )
    )
    val updates = timestamps.tail.map(timestamp => LogTSV(
      timestamp = timestamp,
      action = UPDATE,
      entity = VERTEX(id),
      attributes = attributeFaker
      )
    )
    create ++ updates
  }

  def buildSingleSequenceOnlyUpdates(updateAmount: Int = 5, id: Long): Seq[LogTSV] = {
    buildSingleSequence(updateAmount + 1, id).tail
  }

  def attributeFaker: immutable.HashMap[String, String] = {
    val faker = new Faker()
    immutable.HashMap(
      ("champion", faker.leagueOfLegends().champion()),
      ("friends", faker.friends().character())
    )
  }

}
