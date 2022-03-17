import org.scalatest.flatspec.AnyFlatSpec

class HelloWorldSpec extends AnyFlatSpec {

  "Hello World" should "have 11 characters" in {
    assert("Hello World".length == 11)
  }
}