package utils

import breeze.stats.distributions.Uniform

import java.time.Instant
import scala.language.implicitConversions

object TimeUtils {
  def getRandomOrderedTimestamps(amount: Int, startTime: Instant, endTime: Instant): Seq[Instant] = {
    Uniform(startTime.getEpochSecond, endTime.getEpochSecond)
      .sample(amount)
      .sortWith((t1, t2) => t1 < t2)
      .map(timestamp => Instant.ofEpochSecond(timestamp.toLong))
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
