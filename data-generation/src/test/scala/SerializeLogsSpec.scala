import factories.LogFactory
import thesis.LTSV.{deserializeLTSV, deserializeList, serializeLTSV, serializeList}
import org.scalatest.flatspec.AnyFlatSpec

class SerializeLogsSpec extends AnyFlatSpec {

  "Logs" can "be serialized and then deserialized" in {
    val log = LogFactory().getOne

    val serializedTSV = serializeLTSV(log)
    val deserializedTSV = deserializeLTSV(serializedTSV).get
    assert(log.equals(deserializedTSV))
  }

  it can "be serialized and then deserialized and then serialized" in {
    val log = LogFactory().getOne

    val serializedLog = serializeLTSV(log)
    val deserializedLog = deserializeLTSV(serializedLog).get
    val serializedDeserializedLog = serializeLTSV(deserializedLog)

    assert(serializedLog.equals(serializedDeserializedLog))
  }

  it can "be (de)serialized in list form" in {
    val logs = LogFactory().buildSingleSequence(5, 1)

    assert(deserializeList(serializeList(logs.toList)) == logs.toList)
  }
}