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
import com.abhioncbr.daflow.commons.conf.common.QueryConf
import com.abhioncbr.daflow.commons.conf.extract.ExtractConf
import com.abhioncbr.daflow.commons.conf.extract.ExtractFeedConf
import com.abhioncbr.daflow.commons.conf.extract.ExtractionType
import com.abhioncbr.daflow.job.conf.xml.AttributeTags._
import com.abhioncbr.daflow.job.conf.xml.NodeTags._

object ParseExtract {
  def fromXML(node: scala.xml.NodeSeq): ExtractConf = {
    val extract: ExtractConf =
      ExtractConf(feeds = Array[ExtractFeedConf]((node \ FEED).toList map { s => ParseExtractFeed.fromXML(s) }: _*))
    extract
  }
}

object ParseExtractFeed {
  def fromXML(node: scala.xml.NodeSeq): ExtractFeedConf = {
    val feedName: String = (node \ FEED_NAME).text
    val validateExtractedData: Boolean = ParseUtil.parseBoolean((node \ VALIDATE_EXTRACTED_DATA).text)

    val extractionType: ExtractionType.valueType =
      ExtractionType.getValueType(valueTypeString = (node \ "_").head.label.toUpperCase)

    val attributesMap: Map[String, String] =
      (node \ "_").head.attributes.map(meta => (meta.key, meta.value.toString)).toMap

    val query: Option[QueryConf] = ParseUtil.parseNode[QueryConf](node \ JDBC \ QUERY, None, ParseQuery.fromXML)
    val dataPath: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ FILE_SYSTEM \ DATA_PATH, None, ParseDataPath.fromXML)

    val feed: ExtractFeedConf = ExtractFeedConf(extractFeedName = feedName,
      extractionType = extractionType, extractionAttributesMap = attributesMap,
      dataPath = dataPath, query = query, validateExtractedData = validateExtractedData)
    feed
  }
}
