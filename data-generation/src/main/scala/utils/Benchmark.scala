package utils

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.NANOSECONDS

/** Benchmark
 * Can be used like this:
 * {{{
 *   val b = Benchmark(ConsoleWriter, textPrefix="doStuff")
 *   b.writeHeader
 *   b.benchmarkSingle({spark.doStuff})
 *   b.textPrefix = "SomeOtherFunction"
 *   b.benchmarkAvg({spark.someOtherFunction}, 10)
 *   b.benchmarkAvg({spark.lastFunction},5, "lastFunction")
 *   b.close() // should be called if its a FileWriter
 * }}}
 * textPrefix is a mutable variable that sets the name for the output format
 *
 * @param writer    IoC, write to file or to console
 * @param inSeconds if seconds is the preferred output unit of time (Could actually just be removed)
 */
case class Benchmark(writer: Writer, inSeconds: Boolean = false, textPrefix: String) {
  val header = "function,value,timeunit,number of runs"


  def writeHeader(): Unit = {
    writer.write(header)
  }

  def close(): Unit = writer.close()

  def benchmarkSingle[T](function: => T, textPrefix: String = textPrefix): T = {
    val start = System.nanoTime()
    val returnValue = function
    val end = System.nanoTime()
    val printString = if (inSeconds) {
      s"$textPrefix,${NANOSECONDS.toSeconds(end - start)},s,1"
    } else {
      s"$textPrefix,${NANOSECONDS.toMillis(end - start)},ms,1"
    }
    writer.write(printString)
    returnValue
  }

  def benchmarkAvg[T](function: => T, numberOfRuns: Int = 1, textPrefix: String = textPrefix): Unit = {
    val timings = mutable.MutableList[Long]()
    (1 to numberOfRuns) foreach (_ => {
      val start = System.nanoTime()
      function
      val end = System.nanoTime()
      timings += (start - end)
    })
    val avg = timings.sum / timings.length.toFloat
    val printString = if (inSeconds) {
      s"${textPrefix},${NANOSECONDS.toSeconds(avg.round)},s,$numberOfRuns"
    } else {
      s"$textPrefix,${NANOSECONDS.toMillis(avg.round)},ms,$numberOfRuns"
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
  val pw = new java.io.PrintWriter(Instant.now().toString + "-" + filename + ".csv") // Unique name everytime, will never append to previous run

  override def write(str: String): Unit = {
    pw.append(str + "\n")
  }

  override def close(): Unit = pw.close()
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
