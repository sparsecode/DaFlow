package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.common.file.{FileNameParam, FilePath, PathInfixParam}

object ParseDataPath {
  def fromXML(node: scala.xml.NodeSeq): FilePath = {
    val dataPath: FilePath = FilePath(pathPrefix = Some((node \ "initial_path").text),
        groupPatterns = Some(Array[PathInfixParam]((node \ "group_name_pattern" \ "member").toList map { s => ParseGroupPatterns.fromXML(s) }: _*)),
        feedPattern = Some(ParseFeedPattern.fromXML(node \ "feed_pattern")),
        fileName = Some(ParseFileName.fromXML(node \ "file_name"))
    )
    dataPath
  }
}

object ParseGroupPatterns {
  def fromXML(node: scala.xml.NodeSeq): PathInfixParam = {
    val pathInfixParam : PathInfixParam = PathInfixParam(infixPattern= (node \ "group_name_pattern").text,
      formatInfix = Some(ParseUtil.parseBoolean((node \ "format_group_name_pattern").text)),
      formatInfixArgs = Some(Array[GeneralParam]((node \ "format_values" \ "param").toList map { s => ParseGeneralParams.fromXML(s) }: _*)))
    pathInfixParam
  }
}

object ParseFeedPattern {
  def fromXML(node: scala.xml.NodeSeq): PathInfixParam = {
    val pathInfixParam : PathInfixParam = PathInfixParam( infixPattern= (node \ "feed_name_pattern").text,
      formatInfix = Some(ParseUtil.parseBoolean((node \ "feed_name_pattern").text)),
      formatInfixArgs = Some(Array[GeneralParam]((node \ "format_values" \ "param").toList map { s => ParseGeneralParams.fromXML(s) }: _*)))
    pathInfixParam
  }
}

object ParseFileName {
  def fromXML(node: scala.xml.NodeSeq): FileNameParam = {
    val fileName: FileNameParam = FileNameParam(fileNamePrefix = Some((node \ "file_prefix").text),
      fileNameSuffix = Some((node \ "File_suffix").text), fileNameSeparator = getFileSeparator((node \ "file_separator").text) )
      fileName
   }

  private def getFileSeparator(text: String) : Option[String] = text match {
    case "" => Some(".")
    case _ => Some(text)
  }
}
