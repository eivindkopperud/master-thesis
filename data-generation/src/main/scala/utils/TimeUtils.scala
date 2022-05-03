package utils

import breeze.stats.distributions.Uniform

import java.time.Instant
import scala.language.implicitConversions

object TimeUtils {
  val t1: Instant = Instant.parse("2000-01-01T00:00:00Z")
  val t2: Instant = Instant.parse("2000-01-01T01:00:00Z")
  val t3: Instant = Instant.parse("2000-01-01T02:00:00Z")
  val t4: Instant = Instant.parse("2000-01-01T03:00:00Z")
  val t5: Instant = Instant.parse("2000-01-01T04:00:00Z")
  val t6: Instant = Instant.parse("2000-01-01T05:00:00Z")
  val t7: Instant = Instant.parse("2000-01-01T06:00:00Z")
  val t8: Instant = Instant.parse("2000-01-01T07:00:00Z")


  def getRandomOrderedTimestamps(amount: Int, startTime: Instant, endTime: Instant): Seq[Instant] = {
    Uniform(startTime.getEpochSecond, endTime.getEpochSecond)
      .sample(amount)
      .sortWith((t1, t2) => t1 < t2)
      .map(timestamp => Instant.ofEpochSecond(timestamp.toLong))
  }

  /**
   * Get evenly distributed timestamps.
   * E.g. amount=5, startTime=1, endTime=5 returns [1, 2, 3, 4, 5]
   *
   * @param amount    The number of timestamps wanted
   * @param startTime The first timestamp in the returned value
   * @param endTime   The last timestamp in the returned value
   * @return
   */
  def getDeterministicOrderedTimestamps(amount: Int, startTime: Instant, endTime: Instant): Seq[Instant] = {
    if (amount == 1) {
      return Seq(startTime)
    }
    val step = (endTime.getEpochSecond - startTime.getEpochSecond) / (amount - 1)
    var timestamps = Seq[Instant]()
    for (i <- 0 until amount) {
      timestamps = timestamps :+ Instant.ofEpochSecond(startTime.getEpochSecond + i * step)
    }
    timestamps
  }

  /** Convert Long to Instant
   *
   * If this function is in scope, everytime someone uses Long for a function that
   * take Instant as a parameter, it will be converted
   *
   * getYearFromInstant(x:Instant):Int
   * getYearFromInstant(0) !! Compilation fails
   * import TimeUtils._
   * getYearFromInstant(0) !! 0 get converted to type Instant
   * Crazy stuff
   *
   * @param time
   * @return Instant
   */
  implicit def secondsToInstant(time: Long): Instant = {
    Instant.ofEpochSecond(time)
  }
}
