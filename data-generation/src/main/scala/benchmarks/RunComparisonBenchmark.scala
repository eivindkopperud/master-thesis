package benchmarks

import thesis.DistributionType.{GaussianType, LogNormalType, UniformType}

class RunComparisonBenchmark

object RunComparisonBenchmark {

  def apply(): Unit = {

    val distributions = Seq(LogNormalType(1, 0.4), UniformType(1, 8), GaussianType(4, 2))
    for (dist <- distributions) {
      val simpNameDist = dist.getClass.getSimpleName.filter(_ != '$')
      val benchmarkSuffixes = Seq(s"landy-$simpNameDist", s"snapshot-$simpNameDist")

      new Q1(dist, benchmarkSuffixes = benchmarkSuffixes).run
      new Q2(dist, benchmarkSuffixes = benchmarkSuffixes).run
      new Q3(dist, benchmarkSuffixes = benchmarkSuffixes).run
      new Q4(dist, benchmarkSuffixes = benchmarkSuffixes).run
    }
  }

}
