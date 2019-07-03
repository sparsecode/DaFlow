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

package com.abhioncbr.daflow.job.conf.yaml.config

object YamlTags {
  val NAME: String = "name"
  val TYPE: String = "type"
  val VALUE: String = "value"
  val ORDER: String = "order"
  val JOB_NAME: String = "jobName"
  val FREQUENCY: String = "frequency"
  val FEED_NAME: String = "feedName"
  val SOURCE_NAME: String = "sourceName"
  val TARGET_NAME: String = "targetName"
  val DEFAULT_VALUE: String = "defaultValue"
  val PUBLISH_STATS: String = "publishStats"
  val COALESCE_PARTITION: String = "coalescePartition"
  val OVERWRITE_PARTITION: String = "overwritePartition"
  val VALIDATE_EXTRACTED_DATA: String = "validateExtractedData"
  val COALESCE_PARTITION_COUNT: String = "coalescePartitionCount"
  val VALIDATE_TRANSFORMED_DATA: String = "validateTransformedData"

  // Four root node tags
  val LOAD: String = "load"
  val EXTRACT: String = "extract"
  val TRANSFORM: String = "transform"
  val JOB_STATIC_PARAM: String = "jobStaticParam"

  // General node tags
  val FEED: String = "feed"
  val DATA_PATH: String = "dataPath"

  // Extract node tags
  val JDBC: String = "jdbc"
  val QUERY: String = "query"
  val FILE_SYSTEM: String = "fileSystem"
  val QUERY_PARAMS: String = "queryParams"
  val SQL_QUERY_FILE: String = "sqlQueryFile"
  val CONFIGURATION_FILE: String = "configurationFile"

  // Transform node tags
  val RULE: String = "rule"
  val STEP: String = "step"
  val GROUP: String = "group"
  val CONDITION: String = "condition"

  // Load node tags
  val HIVE: String = "hive"
  val COLUMN: String = "column"
  val PARTITION_DATA: String = "partitionData"
  val PARTITION_COLUMNS: String = "partitionColumns"

  // Other node tags
  val PARAM: String = "param"
  val OTHER_PARAMS: String = "otherParams"
  val FIELD_MAPPING: String = "fieldMapping"

  // Data path node tags
  val PATH: String = "path"
  val MEMBER: String = "member"
  val PREFIX: String = "prefix"
  val SUFFIX: String = "suffix"
  val FILE_NAME: String = "fileName"
  val SEPARATOR: String = "separator"
  val PATH_PATTERN: String = "pathPattern"
  val FEED_PATTERN: String = "feedPattern"
  val INITIAL_PATH: String = "initialPath"
  val GROUP_PATTERN: String = "groupPattern"
  val FORMAT_FEED_NAME: String = "formatFeedName"
  val FEED_NAME_PATTERN: String = "feedNamePattern"
  val FORMAT_ARG_VALUES: String = "formatArgValues"
  val FORMAT_GROUP_NAME: String = "formatGroupName"
  val GROUP_NAME_PATTERN: String = "groupNamePattern"
}
