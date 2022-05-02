package thesis

import org.apache.spark.rdd.RDD
import thesis.Action.{CREATE, DELETE, UPDATE}
import thesis.DataTypes.Attributes
import thesis.Entity.{EDGE, VERTEX}

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.immutable
import scala.io.Source
import scala.util.Try

object LTSV {
  def stringToEntity(s: String): Option[Entity] = {
    s.split(":").toList match {
      case "VERTEX" :: id :: Nil => Some(VERTEX(id.toLong))
      case "EDGE" :: id :: srcId :: dstId :: Nil => Some(EDGE(id.toLong, srcId.toLong, dstId.toLong))
      case _ => None
    }
  }


  /** Serialize a single LogTSV
   *
   * @param logEntry The log entry
   * @return String representation of the LogTSV
   */
  def serializeLTSV(logEntry: LogTSV): String = {
    val timestamp = logEntry.timestamp.toString
    val action = logEntry.action match {
      case CREATE => "CREATE"
      case UPDATE => "UPDATE"
      case DELETE => "DELETE"
    }
    val entity = logEntry.entity match {
      case VERTEX(id) => s"VERTEX:$id"
      case EDGE(id, srcId, dstId) => s"EDGE:$id:$srcId:$dstId"
    }
    val attributes = serializeAttributes(logEntry.attributes)

    List(timestamp, action, entity, attributes)
      .mkString("\t")
  }

  def serializeList(entries: List[LogTSV]): String = entries.map(serializeLTSV).mkString("\n")

  def deserializeList(entries: String): List[LogTSV] = entries.split("\n").flatMap(deserializeLTSV).toList

  /** Deserialize a single LogTSV
   *
   * All the variables with 'serialized' prefix return an Option type.
   * If any return a None the whole block returns a None.
   * If every one of the are parsed correctly it return Some(LogTsv)
   *
   * TODO Rewrite this into Either-type so that information on what is going wrong isn't lost
   *
   * @param logEntry String representation of a log entry
   * @return Possibly a LogTSV
   */
  def deserializeLTSV(logEntry: String): Option[LogTSV] = {
    // This line could fail, but its a hassle to make safe
    // Destructures the first three items, and 'attributes' is the tail
    val timestamp :: action :: entity :: attributes: List[String] = logEntry.split('\t').toList
    for {
      deserializedTimestamp <- Try {
        Instant.parse(timestamp)
      }.toOption
      deserializedAction <- action match {
        case "CREATE" => Some(CREATE)
        case "UPDATE" => Some(UPDATE)
        case "DELETE" => Some(DELETE)
        case _ => None
      }
      deserializedEntry <- stringToEntity(entity)
      deserializedAttributes <- deserializeAttributes(attributes)
    } yield LogTSV(deserializedTimestamp, deserializedAction, deserializedEntry, deserializedAttributes)
  }


  /** Serialize list of attributes
   *
   * Colon and tabs characters are filtered out since they are part of the deserializing process
   *
   * @param attributes List of (Key,Value) representation of each attributes
   * @return String representation of the attributes
   *
   */
  def serializeAttributes(attributes: Attributes): String = {
    val noTabsOrColon = (x: Char) => x != '\t' && x != ':'
    attributes.map(
      tup => s"${tup._1.filter(noTabsOrColon)}:${tup._2.filter(noTabsOrColon)}"
    ).mkString("\t")
  }

  /** Deserialize list of attributes
   *
   * @param attributes List of string representation of each attribute
   * @return Possibly a list of parsed attributes
   */
  def deserializeAttributes(attributes: List[String]): Option[Attributes] = {
    val attributeTuples = attributes
      .map(_.split(':').toList)
      .map {
        case key :: value :: Nil => Some(key, value)
        case _ => None
      }
    if (attributeTuples.forall(_.isDefined)) { // Did everything parse correctly?
      val immutableHashMap = immutable.HashMap[String, String](attributeTuples.flatten: _*)
      Some(immutableHashMap)
    } else {
      None
    }
  }

  def writeToFile(entries: List[LogTSV], filename: String = "logs.tsv"): Unit = {
    val pw = new java.io.PrintWriter(filename)
    pw.write(serializeList(entries))
    pw.close()
  }

  /**
   * Ever wanted to just persist your RDDs of LogTSVs?
   * With this implicit class we add a method to the RDD[LogTSV] class
   * logRDD.serializeLogs() is now possible instead of serializeLogs(logRDDs)
   *
   * @param logs the logs
   */
  implicit class RDDLogTSVMethod(logs: RDD[LogTSV]) {

    def serializeLogs(filename: String = "logs.tsv"): Unit = {
      writeToFile(logs.collect().toList, filename)
    }
  }

  def readFromFile(filename: String = "logs.tsv"): List[LogTSV] = {
    val f = Source.fromFile(filename)
    val list = deserializeList(f.mkString)
    f.close()
    list
  }
}
