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

import com.abhioncbr.daflow.commons.util.FileUtil
import scala.util.Try

import com.abhioncbr.daflow.commons.common.DataPath
import com.abhioncbr.daflow.commons.util.FileUtil

object ParseUtil {
  def parseNode[T](node: scala.xml.NodeSeq, defaultValue: Option[T], fun: scala.xml.NodeSeq => T): Option[T]
  = if (node.nonEmpty){ Some(fun(node)) } else { defaultValue }

  def parseNodeText(node: scala.xml.NodeSeq): String = node.text

  def parseBoolean(node: scala.xml.NodeSeq): Boolean = parseBoolean(node.text)
  def parseBoolean(text: String): Boolean = Try(text.toBoolean).getOrElse(false)

  def parseInt(node: scala.xml.NodeSeq): Int = parseInt(node.text)
  def parseInt(text: String): Int = Try(text.toInt).getOrElse(-1)

  def parseFilePathString(node: scala.xml.NodeSeq): Either[DataPath, String] = parseFilePathString(node.text)
  def parseFilePathString(text: String, fileNameSeparator: String = "."): Either[DataPath, String]
  = FileUtil.getFilePathObject(text, fileNameSeparator)
}
