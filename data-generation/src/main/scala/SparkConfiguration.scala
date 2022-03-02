import org.apache.spark.{SparkConf, SparkContext}

object SparkConfiguration {

  def getSparkContext: SparkContext = {
    val conf = new SparkConf()
      .setAppName("data-generation")
      .setMaster("local")
      .set("spark.ui.enabled", "false")
    new SparkContext(conf)
  }


}
