/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.abhioncbr.daflow.jobConf.xml

import com.abhioncbr.daflow.commons.common.DataPath
import com.abhioncbr.daflow.commons.common.FileNameParam
import com.abhioncbr.daflow.commons.common.GeneralParamConf
import com.abhioncbr.daflow.commons.common.PathInfixParam
import com.typesafe.scalalogging.Logger

object ParseDataPath {
  private val logger = Logger(this.getClass)

  def fromXML(node: scala.xml.NodeSeq): DataPath = {
    val parsedPath = ParseUtil.parseNode[Either[DataPath, String]](node \ "path", None, ParseUtil.parseFilePathString)
    val parsedPathPattern = ParseUtil.parseNode[DataPath](node \ "pathPattern", None, ParseDataPath.parsePathPattern)

    if(parsedPath.isDefined) {
      val dataPath: DataPath = parsedPath.get match {
        case Left(output) => output
        // TODO/FIXME : Handling exception
        case Right(message) => logger.warn(s"[ParseDataPath: fromXML: ] - $message"); null
      }
      dataPath
    } else { parsedPathPattern.get }
  }

  def parsePathPattern(node: scala.xml.NodeSeq): DataPath = {
    val dataPath: DataPath = DataPath(pathPrefix = Some((node \ "initialPath").text),
      cataloguePatterns = ParseUtil.parseNode[Array[PathInfixParam]](node \ "groupPattern", None, ParseGroupPatterns.fromXML),
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
      infixPattern = (node \ "groupNamePattern").text,
      formatInfix = ParseUtil.parseNode[Boolean](node \ "formatGroupName", None, ParseUtil.parseBoolean),
      formatInfixArgs = ParseUtil.parseNode[Array[GeneralParamConf]](node \ "formatArgValues", None, ParseGeneralParams.fromXML) )
    pathInfixParam
  }
}

object ParseFeedPattern {
  def fromXML(node: scala.xml.NodeSeq): PathInfixParam = {
    val pathInfixParam : PathInfixParam = PathInfixParam(infixPattern = (node \ "feedNamePattern").text,
      formatInfix = ParseUtil.parseNode[Boolean](node \ "formatFeedName", None, ParseUtil.parseBoolean),
      formatInfixArgs = ParseUtil.parseNode[Array[GeneralParamConf]](node \ "formatArgValues", None, ParseGeneralParams.fromXML))
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
