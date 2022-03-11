import Action.{CREATE, DELETE, UPDATE}
import Entity.{EDGE, VERTEX}
import LTSV.Attributes
import com.github.javafaker.Faker

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.immutable.HashMap
import scala.util.Try
import scala.util.Random.nextInt
import scala.io.Source

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
  final case class VERTEX(objId:Long) extends Entity
  final case class EDGE(srcId:Long, dstId:Long) extends Entity
}

// TODO create EdgeLogTSV and VertexLogTSV
// toEdgeLog(l:LogTSV): Option[EdgeLogTSV]
/** LogTSV
 *
 * Everything is built around this case class
 * @param timestamp When did the log_entry happen?
 * @param action Type of action
 * @param objectId Id of the object
 * @param attributes List of (Key,Value) attributes relevant to the entry
 */
case class LogTSV(
                 sequentialId: Int,
                 timestamp : Instant,
                 action : Action,
                 objectType: Entity,
                 attributes: Attributes
                 )

object LTSV {

  /** Random LogTSV instance
   *
   * Convenient for testing
   * @return An instance of LogTSV
   */
  def randomLTSV():LogTSV = {
    val actions = List(CREATE, UPDATE, DELETE)
    val faker = new Faker()
    LogTSV(
      timestamp = Instant.now(),
      action = actions(nextInt(actions.size)),
      objectType = VERTEX(nextInt(10000)),
      attributes = List(("champion",faker.leagueOfLegends().champion()),
        ("friends", faker.friends().character()))
    )



  }
  // Type alias
  type Attributes = HashMap[String, String]

  /** Serialize a single LogTSV
   *
   * @param logEntry The log entry
   * @return String representation of the LogTSV
   */
  def serializeLTSV(logEntry:LogTSV ): String ={
    val timestamp = LocalDateTime.ofInstant(logEntry.timestamp, ZoneOffset.UTC).toString +"Z"
    val action = logEntry.action match {
      case CREATE => "CREATE"
      case UPDATE => "UPDATE"
      case DELETE => "DELETE"
    }
    val objectType = logEntry.objectType match {
      case Entity.VERTEX(id) => "EDGE"
      case Entity.EDGE(srcId, dstId) => "VERTEX"
    }
    val attributes = logEntry.objectType match {
      case VERTEX(objId) => serializeAttributes(("objId",objId.toString)::logEntry.attributes)
      case EDGE(srcId, dstId) => serializeAttributes(("srcId",srcId.toString)::("dstId",dstId.toString)::logEntry.attributes)
    }

    List(timestamp, action, objectType, attributes)
      .mkString("\t")
  }

  def serializeList(entries: List[LogTSV]):String = entries.map(serializeLTSV).mkString("\n")
  def deserializeList(entries: String):List[LogTSV] = entries.split("\n").flatMap(deserializeLTSV).toList

  /**  Deserialize a single LogTSV
   *
   * Check this shit out. Monadic error handling
   *
   * All the variables with s_ prefix return an Option type.
   * If any return a None the whole block returns a None.
   * If every one of the are parsed correctly it return Some(LogTsv)
   *
   * TODO Rewrite this into Either-type so that information on what is going wrong isn't lost
   *
   * @param logEntry String representation of a log entry
   * @return Possibly a LogTSV
   */
  def deserializeLTSV(logEntry:String):Option[LogTSV] = {
    // This line could fail, but its a hassle to make safe
    // Destructures the first three items, and 'attributes' is the tail
    val timestamp::action::objectId::objectType::attributes : List[String]= logEntry.split('\t').toList
    for {
      deserializedTimestamp <- Try {Instant.parse(timestamp)}.toOption
      deserializedAction <- action match {
        case "CREATE" => Some(CREATE)
        case "UPDATE" => Some(UPDATE)
        case "DELETE" => Some(DELETE)
        case _ => None
      }
      deserializedObjectId <- Try{objectId.toLong}.toOption
      deserializedObjectType <- objectType match {
        case "VERTEX" => Some(VERTEX)
        case "EDGE" => Some(EDGE)
        case _ => None
      }
      deserializedAttributes <- deserializeAttributes(attributes)
    } yield LogTSV(deserializedTimestamp, deserializedAction, deserializedObjectId,deserializedObjectType, deserializedAttributes)
  }


  /** Serialize list of attributes
   *
   * Colon and tabs characters are filtered out since they are part of the deserializing process
   *
   * @param attributes List of (Key,Value) representation of each attributes
   * @return String representation of the attributes
   *
   */
  def serializeAttributes(attributes:Attributes): String = {
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
  def deserializeAttributes(attributes: List[String]) : Option[Attributes] = {
    val x = attributes
      .map(_.split(':').toList)
      .map {
        case key :: value :: Nil => Some(key, value)
        case _ => None
      }
    if (x.forall(_.isDefined)){ // Did everything parse correctly?
      Some(x.flatten)
    } else {
      None
    }
  }
   def writeToFile(entries:List[LogTSV], filename:String="logs.tsv"):Unit = {
     val pw = new java.io.PrintWriter("logs.tsv")
     pw.write(serializeList(entries))
     pw.close()
   }
  def readFromFile(filename:String="logs.tsv"):List[LogTSV] = {
    val f = Source.fromFile(filename)
    val list = deserializeList(f.mkString)
    f.close()
    list
  }

  /** Home made testsuite
   *
   * TODO set up a TestSuite for scala
   *
   * Generates some random TSVs and makes sure that serializing and deserializing
   * are inverse functions in relation to each other
   *
   */
  def tests():Unit = {
    for( _ <- 1 to 10){
      val tsv = randomLTSV()
      val s_tsv = serializeLTSV(tsv)
      val d_tsv = deserializeLTSV(s_tsv).get
      val s_d_tsv = serializeLTSV(d_tsv)
      assert(d_tsv == tsv)
      assert(s_d_tsv == s_tsv)
    }
    val x = for (_ <- 1 to 5) yield randomLTSV()
    assert(deserializeList(serializeList(x.toList)) == x.toList)

    writeToFile(x.toList, filename="testLogs.tsv")
    assert(readFromFile(filename = "testLogs.tsv") == x.toList)
  }
}
