case class TemporalEvent[A](
                             from: Long,
                             to: Long,
                             time: A
                           ) extends Serializable {

  def isBefore(event: TemporalEvent[A])(implicit time: A => Ordered[A]): Boolean = {
    this.time < event.time
  }

  def isAfter(event: TemporalEvent[A])(implicit time: A => Ordered[A]): Boolean = {
    this.time > event.time
  }
}
