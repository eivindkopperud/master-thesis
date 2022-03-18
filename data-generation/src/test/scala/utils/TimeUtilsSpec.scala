package utils

import org.scalatest.flatspec.AnyFlatSpec

import java.time.Instant

class TimeUtilsSpec extends AnyFlatSpec {
  val start: Instant = Instant.parse("2000-01-01T00:00:00.000Z")
  val end: Instant = Instant.parse("2001-01-01T00:00:00.000Z")
  val timestamps: Seq[Instant] = TimeUtils.getRandomOrderedTimestamps(amount = 3, startTime = start, endTime = end)

  behavior of "TimeUtils"
  it should "return timestamps in order" in {
    assert(timestamps(0).isBefore(timestamps(1)))
    assert(timestamps(1).isBefore(timestamps(2)))
  }

  it should "return timestamps within the boundaries" in {
    timestamps.foreach(timestamp => {
      assert(timestamp.isAfter(start) && timestamp.isBefore(end))
    })
  }

  it should "return the requested amount of timestamps" in {
  assert(timestamps.length == 3)
  }
}
