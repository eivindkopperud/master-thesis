import factories.LandyGraphFactory
import factories.LandyGraphFactory.createGraph
import thesis.Landy
import org.scalatest.flatspec.AnyFlatSpec
import wrappers.SparkTestWrapper


class LandySpec extends AnyFlatSpec with SparkTestWrapper {

  "Landy" can "be created" in {
    val landy: Landy = new Landy(LandyGraphFactory.createGraph())

    assert(landy.vertices.count() == 5)
    assert(landy.edges.count() == 2)
  }

  it can "give a snapshot" in {
    val landy: Landy = new Landy(createGraph())

    val graph = landy.snapshotAtTime(LandyGraphFactory.t2)

    assert(graph.vertices.count() == 2)
    assert(graph.edges.count() == 1)
  }
}