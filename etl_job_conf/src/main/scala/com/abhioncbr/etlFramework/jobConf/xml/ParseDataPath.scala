package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.common.file.{FileNameParam, FilePath, PathInfixParam}
import com.typesafe.scalalogging.Logger

object ParseDataPath {
  private val logger = Logger(this.getClass)

  def fromXML(node: scala.xml.NodeSeq): Option[FilePath] = {
    val parsedPath =  ParseUtil.parseNode[Either[FilePath,String]](node \ "path", None, ParseUtil.parseFilePathString)
    val parsedPathPattern =  ParseUtil.parseNode[FilePath](node \ "pathPattern", None, ParseDataPath.parsePathPattern)

    if(parsedPath.isDefined) {
      parsedPath.get match {
        case Left(output) => Some(output)
        case Right(message) => logger.warn(s"[ParseDataPath: fromXML: ] - $message"); None
      }
    } else parsedPathPattern
  }

  def parsePathPattern(node: scala.xml.NodeSeq): FilePath = {
    val dataPath: FilePath = FilePath(pathPrefix = Some((node \ "initialPath").text),
      groupPatterns = ParseUtil.parseNode[Array[PathInfixParam]](node \ "groupPattern", None, ParseGroupPatterns.fromXML),
      feedPattern = ParseUtil.parseNode[PathInfixParam](node \ "feedPattern", None, ParseFeedPattern.fromXML),
      fileName = ParseUtil.parseNode[FileNameParam](node \ "fileName", None, ParseFileName.fromXML)
    )
    dataPath
  }
}

object ParseGroupPatterns {
  def fromXML(node: scala.xml.NodeSeq): Array[PathInfixParam] = {
    Array[PathInfixParam]((node \ "member").toList map { s => ParseGroupPattern.fromXML(s) }: _*)
  }
}

object ParseGroupPattern {
  def fromXML(node: scala.xml.NodeSeq): PathInfixParam = {
    val pathInfixParam : PathInfixParam = PathInfixParam(
      order = ParseUtil.parseNode[Int](node \ "order", None, ParseUtil.parseInt),
      infixPattern= (node \ "groupNamePattern").text,
      formatInfix = ParseUtil.parseNode[Boolean](node \ "formatGroupName", None, ParseUtil.parseBoolean),
      formatInfixArgs = ParseUtil.parseNode[Array[GeneralParam]](node \ "formatArgValues", None, ParseGeneralParams.fromXML) )//Some(ParseGeneralParams.fromXML(node, nodeTag= "formatArgValues")))
    pathInfixParam
  }
}

object ParseFeedPattern {
  def fromXML(node: scala.xml.NodeSeq): PathInfixParam = {
    val pathInfixParam : PathInfixParam = PathInfixParam( infixPattern= (node \ "feedNamePattern").text,
      formatInfix = ParseUtil.parseNode[Boolean](node \ "formatFeedName", None, ParseUtil.parseBoolean),
      formatInfixArgs = ParseUtil.parseNode[Array[GeneralParam]](node \ "formatArgValues", None, ParseGeneralParams.fromXML) )//Some(ParseGeneralParams.fromXML(node, nodeTag= "formatArgValues")))
    pathInfixParam
  }
}

object ParseFileName {
  def fromXML(node: scala.xml.NodeSeq): FileNameParam = {
    val fileName: FileNameParam = FileNameParam(
      fileNamePrefix = ParseUtil.parseNode[String](node \ "prefix", None, ParseUtil.parseNodeText),
      fileNameSuffix = ParseUtil.parseNode[String](node \ "suffix", None, ParseUtil.parseNodeText),
      fileNameSeparator = ParseUtil.parseNode[String](node \ "separator", Some("."), ParseUtil.parseNodeText))
      fileName
   }
}
