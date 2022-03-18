package factories

import org.scalatest.flatspec.AnyFlatSpec
import thesis.Action.{CREATE, UPDATE}

class LogFactorySpec extends AnyFlatSpec {

  "LogFactory" should "have CREATE action as first action, then UPDATE" in {
    val logs = LogFactory().buildSingleSequence(updateAmount = 5, 1)

    assert (logs.head.action == CREATE)
    logs.tail.foreach(log => assert(log.action == UPDATE))
  }
}