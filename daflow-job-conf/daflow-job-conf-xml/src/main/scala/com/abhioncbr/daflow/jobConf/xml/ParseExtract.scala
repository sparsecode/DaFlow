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

import com.abhioncbr.daflow.commons
import com.abhioncbr.daflow.commons.common.{DataPath, QueryConf}
import com.abhioncbr.daflow.commons.extract.{ExtractConf, ExtractFeedConf, ExtractionType}
import com.abhioncbr.daflow.commons.extract
import com.abhioncbr.daflow.commons.common.DataPath
import com.abhioncbr.daflow.commons.common.QueryConf
import com.abhioncbr.daflow.commons.extract.ExtractFeedConf
import com.abhioncbr.daflow.commons.extract.ExtractionType

object ParseExtract {
  def fromXML(node: scala.xml.NodeSeq): ExtractConf = {
    val extract: ExtractConf =
      commons.extract.ExtractConf(feeds = Array[ExtractFeedConf]((node \ "feed").toList map { s => ParseExtractFeed.fromXML(s) }: _*))
    extract
  }
}

object ParseExtractFeed {
  def fromXML(node: scala.xml.NodeSeq): ExtractFeedConf = {
    val feedName: String = (node \ "@feedName").text
    val extractionSubType: String = (node \ "_").head.attributes.value.text.toUpperCase

    val validateExtractedData: Boolean = ParseUtil.parseBoolean((node \ "@validateExtractedData").text)

    val extractionType: ExtractionType.valueType =
      ExtractionType.getValueType(valueTypeString = (node \ "_").head.label.toUpperCase)

    val query: Option[QueryConf] = ParseUtil.parseNode[QueryConf](node \ "jdbc" \ "query", None, ParseQuery.fromXML)
    val dataPath: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ "fileSystem" \ "dataPath", None, ParseDataPath.fromXML)

    val feed: ExtractFeedConf = extract.ExtractFeedConf(extractFeedName = feedName,
      extractionType = extractionType, extractionSubType = extractionSubType,
      dataPath = dataPath, query = query, validateExtractedData = validateExtractedData)
    feed
  }
}
