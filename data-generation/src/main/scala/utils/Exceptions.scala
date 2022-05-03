package utils

final case class EntityFilterException(
                                        private val message: String = "Either edges or vertices should be filtered out, but they were not.",
                                        private val cause: Throwable = None.orNull
                                      ) extends Exception(message, cause)
