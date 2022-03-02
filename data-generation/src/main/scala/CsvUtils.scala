import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CsvUtils {
  /*
  [String] -> [TemporalEvent]
  Map csv to case classes
   */
  def textFileToRDDCaseClass(file: String, sc: SparkContext): RDD[TemporalEvent[BigDecimal]] =
    sc.textFile(file)
      .map(_.split(','))
      .map(textToEvents)

  val textToEvents: Array[String] => TemporalEvent[BigDecimal] = row => {

    TemporalEvent(
      from = row(0).toLong,
      to = row(1).toLong,
      time = BigDecimal(row(2))
    )
  }
}