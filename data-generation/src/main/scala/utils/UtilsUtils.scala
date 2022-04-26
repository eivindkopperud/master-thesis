package utils

object UtilsUtils {
  def uuid: Long = java.util.UUID.randomUUID.getLeastSignificantBits & Long.MaxValue
}
