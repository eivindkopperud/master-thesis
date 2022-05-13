import benchmarks.{Q1, Q2, Q3, Q4}
import utils.UtilsUtils.getConfig

object Main extends App {
  getConfig("ENV_VARIABLES_ARE_SET") // Use this line if you want to make sure that env variabels are set
  new Q1().run
  new Q2().run
  new Q3().run
  new Q4().run

}
