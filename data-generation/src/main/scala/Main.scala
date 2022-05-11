import benchmarks.Q1
import utils.{Benchmark, ConsoleWriter}

object Main extends App {
  //getConfig("ENV_VARIABLES_ARE_SET") // Use this line if you want to make sure that env variabels are set
  val b = new Benchmark(ConsoleWriter(), true, "Whole scadoodle")
  b.benchmarkSingle(
    {
      new Q1().run
    }
  )
}
