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

object AttributeTags {
  val NAME: String = "@name"
  val TYPE: String = "@type"
  val VALUE: String = "@value"
  val ORDER: String = "@order"
  val JOB_NAME: String = "@jobName"
  val FREQUENCY: String = "@frequency"
  val FEED_NAME: String = "@feedName"
  val SOURCE_NAME: String = "@sourceName"
  val TARGET_NAME: String = "@targetName"
  val DEFAULT_VALUE: String = "@defaultValue"
  val PUBLISH_STATS: String = "@publishStats"
  val COALESCE_PARTITION: String = "@coalescePartition"
  val OVERWRITE_PARTITION: String = "@overwritePartition"
  val VALIDATE_EXTRACTED_DATA: String = "@validateExtractedData"
  val COALESCE_PARTITION_COUNT: String = "@coalescePartitionCount"
  val VALIDATE_TRANSFORMED_DATA: String = "@validateTransformedData"
}
