import Action.{CREATE, DELETE, UPDATE}
import Entity.{EDGE, VERTEX}
import LTSV.Attributes

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.immutable
import scala.io.Source
import scala.util.Try

/** Action Enum
 *
 */
sealed abstract class Action
object Action{
  final case object CREATE extends Action
  final case object UPDATE extends Action
  final case object DELETE extends Action
}
sealed abstract class Entity
object Entity {
  final case class VERTEX(objId: Long) extends Entity
  final case class EDGE(srcId: Long, dstId: Long) extends Entity
}

/** LogTSV
 *
 * Everything is built around this case class
 * @param timestamp When did the log_entry happen?
 * @param action Type of action
 * @param entity The type of the object with id(s)
 * @param attributes List of (Key,Value) attributes relevant to the entry
 */
case class LogTSV(
                 timestamp : Instant,
                 action : Action,
                 entity: Entity,
                 attributes: Attributes
                 )


object LTSV {
  def stringToEntity(s: String): Option[Entity] = {
    s.split(":").toList match {
      case "VERTEX" :: id :: Nil => Some(VERTEX(id.toLong))
      case "EDGE" :: srcId :: dstId :: Nil => Some(EDGE(srcId.toLong, dstId.toLong))
      case _ => None
    }
  }

  // Type alias
  type Attributes = immutable.HashMap[String, String]

  /** Serialize a single LogTSV
   *
   * @param logEntry The log entry
   * @return String representation of the LogTSV
   */
  def serializeLTSV(logEntry: LogTSV): String = {
    val timestamp = LocalDateTime.ofInstant(logEntry.timestamp, ZoneOffset.UTC).toString + "Z"
    val action = logEntry.action match {
      case CREATE => "CREATE"
      case UPDATE => "UPDATE"
      case DELETE => "DELETE"
    }
    val entity = logEntry.entity match {
      case VERTEX(id) => s"VERTEX:$id"
      case EDGE(srcId, dstId) => s"EDGE:$srcId:$dstId"
    }
    val attributes = serializeAttributes(logEntry.attributes)

    List(timestamp, action, entity, attributes)
      .mkString("\t")
  }

  def serializeList(entries: List[LogTSV]): String = entries.map(serializeLTSV).mkString("\n")
  def deserializeList(entries: String): List[LogTSV] = entries.split("\n").flatMap(deserializeLTSV).toList

  /**  Deserialize a single LogTSV
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
    val timestamp::action::entity::attributes : List[String] = logEntry.split('\t').toList
    for {
      deserializedTimestamp <- Try {Instant.parse(timestamp)}.toOption
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
    val noTabsOrColon = (x:Char) => x != '\t' && x != ':'
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
    if (attributeTuples.forall(_.isDefined)){ // Did everything parse correctly?
      val immutableHashMap = immutable.HashMap[String, String](attributeTuples.flatten:_*)
      Some(immutableHashMap)
    } else {
      None
    }
  }

   def writeToFile(entries: List[LogTSV], filename: String="logs.tsv"): Unit = {
     val pw = new java.io.PrintWriter(filename)
     pw.write(serializeList(entries))
     pw.close()
   }

  def readFromFile(filename: String="logs.tsv"): List[LogTSV] = {
    val f = Source.fromFile(filename)
    val list = deserializeList(f.mkString)
    f.close()
    list
  }
}
