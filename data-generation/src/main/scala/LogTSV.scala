import Action.{CREATE, DELETE, UPDATE}
import LTSV.Attributes

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.util.Try

/**
 * This is apparently the best way to make enums in Scala2
 */
sealed abstract class Action
object Action{
  final case object CREATE extends Action
  final case object UPDATE extends Action
  final case object DELETE extends Action
}

/**
 * Everything is built around this case class
 * @param timestamp
 * @param action
 * @param objectId
 * @param attributes
 */
case class LogTSV(
                 timestamp : Instant,
                 action : Action,
                 objectId: Long,
                 attributes: Attributes
                 )

object LTSV {
  type Attributes = List[(String, String)]
  def serializeLTSV(log_entry:LogTSV ): String ={
    val timestamp = LocalDateTime.ofInstant(log_entry.timestamp, ZoneOffset.UTC).toString
    val action = log_entry.action match {
      case CREATE => "CREATE"
      case UPDATE => "UPDATE"
      case DELETE => "DELETE"
    }
    val objectId = log_entry.objectId.toString
    val attributes = serializeAttributes(log_entry.attributes)
    List(timestamp, action, objectId, attributes)
      .mkString("\t")
  }

  /**
   * TODO Prove that serializeList is the inverse of deserializeList with tests
   * Serializing twice should equal NOP
   * @param entries
   * @return
   */
  def serializeList(entries: List[LogTSV]):String = entries.map(serializeLTSV).mkString("\n")
  def deserializeList(entries: String):List[LogTSV] = entries.split("\n").flatMap(deserializeLTSV).toList

  /**
   * Check this shit out. Monadic error handling
   * All the variables with s_ prefix return an Option type
   * If any return a None the whole block returns a None
   * If every one of the are parsed correctly it return Some(LogTsv)
   *
   * @param log_entry String representation of a log entry
   * @return Possibly a LogTSV
   */
  def deserializeLTSV(log_entry:String):Option[LogTSV] = {
    val timestamp :: action :: objectid :: attributes: Seq[String] = log_entry.split('\t')
    for {
      s_timestamp <- Try {Instant.parse(timestamp)}.toOption
      s_action <- action match {
        case "CREATE" => Some(CREATE)
        case "UPDATE" => Some(UPDATE)
        case "DELETE" => Some(DELETE)
        case _ => None
      }
      s_objectId <- Try{objectid.toLong}.toOption
      s_attributes <- deserializeAttributes(attributes)
    } yield LogTSV(s_timestamp, s_action, s_objectId, s_attributes)
  }

  def serializeAttributes(attributes:Attributes): String = {
    val noTabs = (x:Char) => x != '\t'
    attributes.map(
      tup => s"${tup._1}:${tup._2}"
    ).map(_.filter(noTabs)).mkString("\t")
  }
  def deserializeAttributes(attributes: List[String]) : Option[Attributes] = {
    val handleSingleKeyVal = (x:List[String]) => (x(0), x(1))
    if (attributes.length % 2 != 0) {
      None
    } else {
      // Fuck this doesn't work. I want [1,2,3,4] ->  [1,2][2,3][3,4], not [1,2][2,3][3,4]
      // @Eivind I know you have done this before
      Some(attributes.sliding(2).map(handleSingleKeyVal).toList)
    }
  }
}
