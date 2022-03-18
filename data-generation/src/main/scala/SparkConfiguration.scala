import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfiguration {

  def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("data-generation")
      .setMaster("local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.memory", "2g")
    SparkSession.builder.config(conf).getOrCreate()
  }


}
