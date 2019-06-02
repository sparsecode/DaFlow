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

import com.abhioncbr.daflow.commons.conf.common.GeneralParamConf
import com.abhioncbr.daflow.commons.conf.load.PartitioningDataConf
import com.abhioncbr.daflow.job.conf.xml.AttributeTags._
import com.abhioncbr.daflow.job.conf.xml.NodeTags._

object ParsePartitioningData {
  def fromXML(node: scala.xml.NodeSeq): PartitioningDataConf = {
    val coalesce = ParseUtil.parseBoolean((node \ COALESCE_PARTITION).text)
    val overwrite = ParseUtil.parseBoolean((node \ OVERWRITE_PARTITION).text)
    val coalesceCount = ParseUtil.parseInt((node \ COALESCE_PARTITION_COUNT).text)
    val partitionColumns = List[GeneralParamConf]((node \ PARTITION_COLUMNS \ COLUMN).
      toList map { s => ParseGeneralParam.fromXML(s) }: _*)

    PartitioningDataConf(coalesce = coalesce, overwrite = overwrite,
      coalesceCount = coalesceCount, partitionColumns = partitionColumns)
  }
}
