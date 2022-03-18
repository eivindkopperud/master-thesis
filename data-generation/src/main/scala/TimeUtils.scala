import breeze.stats.distributions.Uniform

import java.time.Instant

object TimeUtils {
  def getRandomOrderedTimestamps(amount: Int, startTime: Instant, endTime: Instant): Seq[Instant] = {
    Uniform(startTime.getEpochSecond, endTime.getEpochSecond)
      .sample(amount)
      .sortWith((t1, t2) => t1 < t2)
      .map(timestamp => Instant.ofEpochSecond(timestamp.toLong))
  }
}
