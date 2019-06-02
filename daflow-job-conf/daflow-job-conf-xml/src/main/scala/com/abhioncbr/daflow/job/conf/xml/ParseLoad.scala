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
import com.abhioncbr.daflow.commons.conf.load.LoadConf
import com.abhioncbr.daflow.commons.conf.load.LoadFeedConf
import com.abhioncbr.daflow.commons.conf.load.LoadType
import com.abhioncbr.daflow.commons.conf.load.PartitioningDataConf
import com.abhioncbr.daflow.job.conf.xml.AttributeTags._
import com.abhioncbr.daflow.job.conf.xml.NodeTags._

object ParseLoad {
  def fromXML(node: scala.xml.NodeSeq): LoadConf = {
    val load: LoadConf = LoadConf(feeds =
      Array[LoadFeedConf]((node \ FEED).toList map { s => ParseLoadFeed.fromXML(s) }: _*))
    load
  }
}

object ParseLoadFeed {
  def fromXML(node: scala.xml.NodeSeq): LoadFeedConf = {
    val loadFeedName: String = (node \ NAME).text
    val loadType: LoadType.valueType =
      LoadType.getValueType(valueTypeString = (node \ "_").head.label.toUpperCase)

    val attributesMap: Map[String, String] = (node \ "_").head.attributes.map(meta => (meta.key, meta.value.toString)).toMap
    val dataPath: DataPath = ParseUtil.parseNode[DataPath](node \ "_" \ DATA_PATH, None, ParseDataPath.fromXML).orNull
    val partitioningData: Option[PartitioningDataConf] =
      ParseUtil.parseNode[PartitioningDataConf](node \ HIVE \ PARTITION_DATA, None, ParsePartitioningData.fromXML)

    val feed: LoadFeedConf = LoadFeedConf(loadFeedName = loadFeedName,
      loadType = loadType, attributesMap = attributesMap, dataPath = dataPath, partitioningData = partitioningData)

    feed
  }
}
