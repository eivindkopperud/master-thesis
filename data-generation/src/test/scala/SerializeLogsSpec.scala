import LTSV.{deserializeLTSV, deserializeList, randomLTSV, serializeLTSV, serializeList}
import org.scalatest.flatspec.AnyFlatSpec

class SerializeLogsSpec extends AnyFlatSpec {

  "Logs" can "be serialized and then deserialized" in {
    val log = randomLTSV()

    val serializedTSV = serializeLTSV(log)
    val deserializedTSV = deserializeLTSV(serializedTSV).get
    assert(log.equals(deserializedTSV))
  }

  it can "be serialized and then deserialized and then serialized" in {
    val log = randomLTSV()

    val serializedLog = serializeLTSV(log)
    val deserializedLog = deserializeLTSV(serializedLog).get
    val serializedDeserializedLog = serializeLTSV(deserializedLog)

    assert(serializedLog.equals(serializedDeserializedLog))
  }

  it can "be (de)serialized in list form" in {
    val logs = for (_ <- 1 to 5) yield randomLTSV()

    assert(deserializeList(serializeList(logs.toList)) == logs.toList)
  }
}