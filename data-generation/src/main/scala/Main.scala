import thesis.DataSource
import thesis.SparkConfiguration.getSparkSession
import thesis.TopologyGraphGenerator.generateCsvThreshold
import utils.UtilsUtils.getConfig

object Main extends App {
  getConfig("ENV_VARIABLES_ARE_SET") // Use this line if you want to make sure that env variabels are set
  //  new Q1().run
  //  new Q2().run
  //  new Q3().run
  //  new Q4().run
  implicit val spark = getSparkSession
  implicit val sc = spark.sparkContext

  generateCsvThreshold(DataSource.Reptilian)
  generateCsvThreshold(DataSource.ContactsHyperText)
  generateCsvThreshold(DataSource.FbMessages)
  spark.close()

  //val g = addGraphUpdateDistribution(graph, UniformType(0, 2))

}
