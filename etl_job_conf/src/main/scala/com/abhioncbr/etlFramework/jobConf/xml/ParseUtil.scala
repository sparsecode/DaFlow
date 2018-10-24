package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.util.FileUtil

import scala.util.Try
object ParseUtil {
  def nodeExists[T](node: scala.xml.NodeSeq, defaultValue: T, fun: ()=> T): T = if(node.nonEmpty) fun() else defaultValue

  def parseNodeText(node: scala.xml.NodeSeq, defaultValue: Option[String] = None): Option[String] = if(node.nonEmpty) Some(node.text) else defaultValue

  def parseBoolean(text: String): Boolean = Try(text.toBoolean).getOrElse(false)

  def parseInt(text: String): Int = Try(text.toInt).getOrElse(-1)

  def parseFilePathString(text: String, fileNameSeparator: String = "."): Either[FilePath, String] = FileUtil.getFilePathObject(text, fileNameSeparator)
}
