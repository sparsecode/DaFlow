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

package com.abhioncbr.daflow.job.conf.xml

import com.abhioncbr.daflow.commons.conf.common.DataPath
import com.abhioncbr.daflow.commons.conf.common.FileNameParam
import com.abhioncbr.daflow.commons.conf.common.GeneralParamConf
import com.abhioncbr.daflow.commons.conf.common.PathInfixParam
import com.abhioncbr.daflow.job.conf.xml.NodeTags._
import com.typesafe.scalalogging.Logger

object ParseDataPath {
  private val logger = Logger(this.getClass)

  def fromXML(node: scala.xml.NodeSeq): DataPath = {
    val parsedPath = ParseUtil.parseNode[Either[DataPath, String]](node \ PATH, None, ParseUtil.parseFilePathString)
    val parsedPathPattern = ParseUtil.parseNode[DataPath](node \ PATH_PATTERN, None, ParseDataPath.parsePathPattern)

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
    val dataPath: DataPath = DataPath(pathPrefix = Some((node \ INITIAL_PATH).text),
      cataloguePatterns = ParseUtil.parseNode[Array[PathInfixParam]](node \ GROUP_PATTERN, None, ParseGroupPatterns.fromXML),
      feedPattern = ParseUtil.parseNode[PathInfixParam](node \ FEED_PATTERN, None, ParseFeedPattern.fromXML),
      fileName = ParseUtil.parseNode[FileNameParam](node \ FILE_NAME, None, ParseFileName.fromXML)
    )
    dataPath
  }
}

object ParseGroupPatterns {
  def fromXML(node: scala.xml.NodeSeq): Array[PathInfixParam] = {
    Array[PathInfixParam]((node \ MEMBER).toList map { s => ParseGroupPattern.fromXML(s) }: _*)
  }
}

object ParseGroupPattern {
  def fromXML(node: scala.xml.NodeSeq): PathInfixParam = {
    val pathInfixParam : PathInfixParam = PathInfixParam(
      order = ParseUtil.parseNode[Int](node \ ORDER, None, ParseUtil.parseInt),
      infixPattern = (node \ GROUP_NAME_PATTERN).text,
      formatInfix = ParseUtil.parseNode[Boolean](node \ FORMAT_GROUP_NAME, None, ParseUtil.parseBoolean),
      formatInfixArgs = ParseUtil.parseNode[Array[GeneralParamConf]](node \ FORMAT_ARG_VALUES, None, ParseGeneralParams.fromXML)
    )
    pathInfixParam
  }
}

object ParseFeedPattern {
  def fromXML(node: scala.xml.NodeSeq): PathInfixParam = {
    val pathInfixParam : PathInfixParam = PathInfixParam(infixPattern = (node \ FEED_NAME_PATTERN).text,
      formatInfix = ParseUtil.parseNode[Boolean](node \ FORMAT_FEED_NAME, None, ParseUtil.parseBoolean),
      formatInfixArgs = ParseUtil.parseNode[Array[GeneralParamConf]](node \ FORMAT_ARG_VALUES, None, ParseGeneralParams.fromXML))
    pathInfixParam
  }
}

object ParseFileName {
  def fromXML(node: scala.xml.NodeSeq): FileNameParam = {
    val fileName: FileNameParam = FileNameParam(
      fileNamePrefix = ParseUtil.parseNode[String](node \ PREFIX, None, ParseUtil.parseNodeText),
      fileNameSuffix = ParseUtil.parseNode[String](node \ SUFFIX, None, ParseUtil.parseNodeText),
      fileNameSeparator = ParseUtil.parseNode[String](node \ SEPARATOR, Some("."), ParseUtil.parseNodeText))
      fileName
  }
}
