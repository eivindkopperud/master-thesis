package benchmarks

import thesis.CorrelationMode
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

  /** Testing negative, uniform and positive correlation
   *
   */
  def runQ9(): Unit = {

    val dist = LogNormalType(1, 0.4)
    val correlations = Seq(CorrelationMode.NegativeCorrelation, CorrelationMode.Uniform, CorrelationMode.PositiveCorrelation)
    for (corr <- correlations) {
      val simpNameDist = dist.getClass.getSimpleName.filter(_ != '$')
      val simpNameCorr = corr.getClass.getSimpleName.filter(_ != '$')
      val benchmarkSuffixes = Seq(s"landy-$simpNameDist-$corr", s"snapshot-$simpNameDist-$corr")

      new Q1(dist, benchmarkSuffixes = benchmarkSuffixes, correlationMode = corr).run
      new Q2(dist, benchmarkSuffixes = benchmarkSuffixes, correlationMode = corr).run
      new Q3(dist, benchmarkSuffixes = benchmarkSuffixes, correlationMode = corr).run
      new Q4(dist, benchmarkSuffixes = benchmarkSuffixes, correlationMode = corr).run
    }
  }

}
