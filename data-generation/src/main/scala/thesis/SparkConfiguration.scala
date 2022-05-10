package thesis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.UtilsUtils.getConfigSafe

object SparkConfiguration {

  def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("data-generation")
      .setMaster(getConfigSafe("SparkConfigurationMaster").getOrElse("local[2]"))
      .set("spark.driver.memory", "2g")
    SparkSession.builder.config(conf).getOrCreate()
  }
  //.setMaster("local[2]")
  //.setMaster("spark://spark:7077")
  // use the line above when making an uberjar
}
