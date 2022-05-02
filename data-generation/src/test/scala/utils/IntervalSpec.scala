package utils

import org.scalatest.flatspec.AnyFlatSpec
import thesis.Interval

import java.time.Instant

class IntervalSpec extends AnyFlatSpec {

  val t1: Instant = Instant.parse("2000-01-01T00:00:00Z")
  val t2: Instant = Instant.parse("2000-01-01T01:00:00Z")
  val t3: Instant = Instant.parse("2000-01-01T02:00:00Z")
  val t4: Instant = Instant.parse("2000-01-01T03:00:00Z")
  val t5: Instant = Instant.parse("2000-01-01T04:00:00Z")

  "Intervals" can "overlap other intervals" in {
    assert(Interval(t1, t3).overlaps(Interval(t2, t4)))
    assert(Interval(t2, t4).overlaps(Interval(t1, t3)))
  }

  it can "overlap inclusively" in {
    assert(Interval(t2, t3).overlaps(Interval(t1, t2)))
    assert(Interval(t2, t3).overlaps(Interval(t3, t4)))
  }

  it should "not overlap non-overlapping intervals" in {
    assert(!Interval(t1, t3).overlaps(Interval(t4, t5)))
  }

  it can "contain points" in {
    assert(Interval(t1, t3).contains(t2))
  }

  it can "contain points in time inclusively" in {
    assert(Interval(t1, t2).contains(t1))
    assert(Interval(t1, t2).contains(t2))
  }

  it can "contain other intervals" in {
    assert(Interval(t1, t4).contains(Interval(t2, t3)))
  }

  it can "contain itself" in {
    val interval = Interval(t1, t2)
    assert(interval.contains(interval))
  }

  it should "not contain overlapping intervals" in {
    assert(!Interval(t1, t3).contains(Interval(t2, t4)))
  }

  it should "not contain non-overlapping intervals" in {
    assert(!Interval(t1, t2).contains(Interval(t3, t4)))
  }
}
