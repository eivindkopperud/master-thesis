package utils

import org.scalatest.flatspec.AnyFlatSpec
import thesis.Interval
import utils.TimeUtils.{t1, t2, t3, t4, t5}

class IntervalSpec extends AnyFlatSpec {
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
