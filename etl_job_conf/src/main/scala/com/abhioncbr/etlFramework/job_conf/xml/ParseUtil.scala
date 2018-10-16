package com.abhioncbr.etlFramework.job_conf.xml

object ParseUtil {

  def parseBoolean(text: String): Boolean = {
    text match {
      case "" => false
      case _ => text.toBoolean
    }
  }

  def parseInt(text: String): Int = {
    text match {
      case "" => 0
      case _ => text.toInt
    }
  }
}
