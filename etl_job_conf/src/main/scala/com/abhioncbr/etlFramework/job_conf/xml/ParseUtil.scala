package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.util.FileUtil

import scala.util.Try
object ParseUtil {
  def parseBoolean(text: String): Boolean = Try(text.toBoolean).getOrElse(false)
  def parseInt(text: String): Int = Try(text.toInt).getOrElse(-1)
  def parseFilePathString(text: String): Option[FilePath] = text match {
    case "" => None
    case _ => Some(FileUtil.getFilePathObject(text))
  }
}
