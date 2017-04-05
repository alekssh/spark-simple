package com.sh.scala.dataframe

import java.sql.Timestamp

/**
  * Created by alex on 4/5/17.
  */
object StringImplicits {

  implicit class StringImprovements(val s: String) {

    import scala.util.control.Exception.catching

    def toIntSafe = catching(classOf[NumberFormatException]).opt(s.toInt)

    def toLongSafe = catching(classOf[NumberFormatException]).opt(s.toLong)

    def toTimestampSafe = catching(classOf[IllegalArgumentException]).opt(Timestamp.valueOf(s))
  }

}
