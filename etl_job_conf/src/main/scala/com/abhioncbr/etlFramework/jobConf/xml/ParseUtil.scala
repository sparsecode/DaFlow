package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.util.FileUtil

import scala.util.Try
object ParseUtil {
  def parseNode[T](node: scala.xml.NodeSeq, defaultValue: Option[T], fun: scala.xml.NodeSeq => T): Option[T] = if(node.nonEmpty) Some(fun(node)) else defaultValue

  def parseNodeText(node: scala.xml.NodeSeq): String = node.text

  def parseBoolean(node: scala.xml.NodeSeq): Boolean = parseBoolean(node.text)
  def parseBoolean(text: String): Boolean = Try(text.toBoolean).getOrElse(false)

  def parseInt(node: scala.xml.NodeSeq): Int = parseInt(node.text)
  def parseInt(text: String): Int = Try(text.toInt).getOrElse(-1)

  def parseFilePathString(node: scala.xml.NodeSeq): Either[FilePath, String] = parseFilePathString(node.text)
  def parseFilePathString(text: String, fileNameSeparator: String = "."): Either[FilePath, String] = FileUtil.getFilePathObject(text, fileNameSeparator)
}
