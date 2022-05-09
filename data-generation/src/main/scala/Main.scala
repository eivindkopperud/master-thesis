import thesis.SparkConfiguration.getSparkSession

object Main extends App {
  val sc = getSparkSession
  val rdd = sc.sparkContext.parallelize(Seq.range(0, 100000))
  //PossibleWorkFlow.run()
  println(rdd.count())
}
