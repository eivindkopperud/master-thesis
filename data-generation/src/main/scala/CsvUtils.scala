import com.sun.tools.javac.code.TypeTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe.typeOf

object CsvUtils {
  /*
  [String] -> [TemporalEvent]
  Map csv to case classes
   */
  def textFileToRDDCaseClass(file: String, sc: SparkContext): RDD[TemporalEvent[BigDecimal]] = {
    val rdd = sc.textFile(file).map(_.split(',')): RDD[Array[String]]

    rdd.map(textToEvents)
  }
  val textToEvents: Array[String] => TemporalEvent[BigDecimal] = row => {

    TemporalEvent(
      from = row(0).toLong,
      to = row(1).toLong,
      time = BigDecimal(row(2))
    )
  }
}