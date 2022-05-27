import benchmarks.{Q1DegreeBenchmark, RunComparisonBenchmark}
import utils.UtilsUtils.getConfig

object Main extends App {
  getConfig("ENV_VARIABLES_ARE_SET") // Use this line if you want to make sure that env variabels are set
  new Q1DegreeBenchmark().run
  //RunComparisonBenchmark()

}
