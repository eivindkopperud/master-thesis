package wrappers

import org.apache.spark.sql.SparkSession

trait SparkTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .config("spark.sql.shuffle.partitions", "1") // To enhance performance, see https://mrpowers.medium.com/how-to-cut-the-run-time-of-a-spark-sbt-test-suite-by-40-52d71219773f
      .getOrCreate()
  }
}
