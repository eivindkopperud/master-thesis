package utils

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.NANOSECONDS

/** Benchmark
 * Can be used like this:
 * {{{
 *   val b = Benchmark(ConsoleWriter, textPrefix="doStuff", customColumn="number of something")
 *   b.writeHeader
 *   b.benchmarkSingle({spark.doStuff}, customColum={someCalculation})
 *   b.textPrefix = "SomeOtherFunction"
 *   b.benchmarkAvg({spark.someOtherFunction}, 10, customColum={someCalculation}))
 *   b.benchmarkAvg({spark.lastFunction},5, "lastFunction", customColum={someCalculation}))
 *   b.close() // should be called if its a FileWriter
 * }}}
 * textPrefix is a mutable variable that sets the name for the output format
 *
 * @param writer       IoC, write to file or to console
 * @param inSeconds    if seconds is the preferred output unit of time
 * @param textPrefix   The function that is run
 * @param customColumn A column where the benchmark user decides the content
 */
case class Benchmark(writer: Writer, inSeconds: Boolean = false, textPrefix: String, customColumn: String = "") {
  val header = s"function,value,timeunit,number of runs,$customColumn"


  def writeHeader(): Unit = {
    writer.write(header)
  }

  def close(): Unit = writer.close()

  def benchmarkSingle[T](function: => T, textPrefix: String = textPrefix, customColumnValue: String = ""): T = {
    val start = System.nanoTime()
    val returnValue = function
    val end = System.nanoTime()
    val printString = if (inSeconds) {
      s"$textPrefix,${NANOSECONDS.toSeconds(end - start)},s,1,$customColumnValue"
    } else {
      s"$textPrefix,${NANOSECONDS.toMillis(end - start)},ms,1,$customColumnValue"
    }
    writer.write(printString)
    returnValue
  }

  def benchmarkAvg[T](function: => T, numberOfRuns: Int = 1, textPrefix: String = textPrefix, customColumnValue: String = ""): Unit = {
    val timings = mutable.MutableList[Long]()
    (1 to numberOfRuns) foreach (_ => {
      val start = System.nanoTime()
      function
      val end = System.nanoTime()
      timings += (end - start)
    })
    val avg = timings.sum / timings.length.toFloat
    val printString = if (inSeconds) {
      s"$textPrefix,${NANOSECONDS.toSeconds(avg.round)},s,$numberOfRuns,$customColumnValue"
    } else {
      s"$textPrefix,${NANOSECONDS.toMillis(avg.round)},ms,$numberOfRuns,$customColumnValue"
    }
    writer.write(printString)
  }

}

trait Writer {
  def write(str: String): Unit

  def close(): Unit
}

case class ConsoleWriter() extends Writer {
  override def write(str: String): Unit = println(str)

  override def close(): Unit = println("CLOSED")
}

case class FileWriter(filename: String) extends Writer {
  val pw = new java.io.PrintWriter(getFilePath(filename))

  override def write(str: String): Unit = {
    pw.append(str + "\n")
  }

  override def close(): Unit = pw.close()

  /**
   * Get the absolute file path to the benchmark output.
   * Requires the user to have set the benchmark directory outside the root folder
   * of the the project (master-thesis).
   *
   * @return A unique, absolute filepath
   */
  private def getFilePath(filename: String): String = {
    val projectLocation = new java.io.File("../../").getCanonicalPath
    val benchmarkDirectory = "/benchmarks/"
    val timePrefix = Instant.now()
      .toString
      .split("\\.")(0) // Get the format YYYY-mm-ddTHH:MM:SS
      .replace(":", "-")
    s"$projectLocation$benchmarkDirectory$timePrefix-$filename.csv"
  }
}

case class BothWriter(filename: String) extends Writer {
  val file: FileWriter = FileWriter(filename)
  val console: ConsoleWriter = ConsoleWriter()

  override def write(str: String): Unit = {
    file.write(str)
    console.write(str)
  }

  override def close(): Unit = file.close()
}
