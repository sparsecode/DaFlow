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

package com.abhioncbr.daflow.commons

import com.abhioncbr.daflow.commons.common.{DataPath, FileNameParam, GeneralParamConf, PathInfixParam}
import com.abhioncbr.daflow.commons.common.DataPath
import com.abhioncbr.daflow.commons.common.FileNameParam
import com.abhioncbr.daflow.commons.common.GeneralParamConf
import com.abhioncbr.daflow.commons.common.PathInfixParam

object Fixture {
  // fixture around data path-prefix
  val pathPrefix: String = "/pathPrefix_1/pathPrefix_2"
  val tildePathPrefix: String = "~/pathPrefix_1"
  val pathPrefixWithEndSeparator: String = "/pathPrefix_1/pathPrefix_2/"

  // fixture around catalogue patterns
  val catalogueStaticInfixPattern1: String = "catalogue_1"
  val catalogueStaticInfixPattern2: String = "catalogue_2"
  val catalogueInfixPattern1: String = "catalogue_%s"
  val catalogueInfixPattern2: String = "catalogue_%s_%s"

  val catalogueInfixPatternFormatArg1: GeneralParamConf = GeneralParamConf(1, "arg1", "arg1_value", "arg1_default_value" )
  val catalogueInfixPatternFormatArg2: GeneralParamConf = GeneralParamConf(2, "arg2", "arg2_value", "arg2_default_value" )
  val catalogueFormatArgs1: Option[Array[GeneralParamConf]] = Some(Array(catalogueInfixPatternFormatArg1))
  val catalogueFormatArgs2:
    Option[Array[GeneralParamConf]] = Some(Array(catalogueInfixPatternFormatArg1, catalogueInfixPatternFormatArg2))

  val cataloguePattern1: PathInfixParam = PathInfixParam(Some(1), catalogueStaticInfixPattern1)
  val cataloguePattern2: PathInfixParam = PathInfixParam(Some(2), catalogueStaticInfixPattern2)
  val cataloguePattern3: PathInfixParam = PathInfixParam(Some(3), catalogueInfixPattern1, Some(true), catalogueFormatArgs1)
  val cataloguePattern4: PathInfixParam = PathInfixParam(Some(4), catalogueInfixPattern2, Some(true), catalogueFormatArgs2)

  val cataloguePatterns1: Option[Array[PathInfixParam]] = Some(Array(cataloguePattern1))
  val cataloguePatterns2: Option[Array[PathInfixParam]] = Some(Array(cataloguePattern1, cataloguePattern2))
  val cataloguePatterns3: Option[Array[PathInfixParam]] = Some(Array(cataloguePattern3, cataloguePattern4))
  val cataloguePatterns4: Option[Array[PathInfixParam]] = Some(Array(cataloguePattern1, cataloguePattern3))

  // fixture around feed patterns
  val feedStaticInfixParam: String = "feed_1"
  val feedInfixPattern1: String = "feed_%s"
  val feedInfixPattern2: String = "feed_%s_%s"

  val feedInfixPatternFormatArg1: GeneralParamConf = GeneralParamConf(1, "arg1", "arg1_value", "arg1_default_value" )
  val feedInfixPatternFormatArg2: GeneralParamConf = GeneralParamConf(2, "arg2", "arg2_value", "arg2_default_value" )
  val feedFormatArgs1: Option[Array[GeneralParamConf]] = Some(Array(feedInfixPatternFormatArg1))
  val feedFormatArgs2: Option[Array[GeneralParamConf]] = Some(Array(feedInfixPatternFormatArg1, feedInfixPatternFormatArg2))

  val feedPattern1: PathInfixParam = PathInfixParam(infixPattern = feedStaticInfixParam)
  val feedPattern2: PathInfixParam = PathInfixParam(infixPattern = feedInfixPattern1, formatInfix = Some(true),
    formatInfixArgs = feedFormatArgs1)
  val feedPattern3: PathInfixParam = PathInfixParam(infixPattern = feedInfixPattern2, formatInfix = Some(true),
    formatInfixArgs = feedFormatArgs2)

  // fixture around file param name
  val fileNamePrefix1: String = "fileNamePrefix"
  val fileNamePrefix2: String = "*"

  val fileNameSuffix1: String = "fileNameSuffix1"
  val fileNameSuffix2: String = "*"

  val fileNameSeparator: String = ";"

  val fileNameParam1: FileNameParam = FileNameParam(Some(fileNamePrefix1), Some(fileNameSuffix1))
  val fileNameParam2: FileNameParam = FileNameParam(Some(fileNamePrefix1), Some(fileNameSuffix2))
  val fileNameParam3: FileNameParam = FileNameParam(Some(fileNamePrefix2), Some(fileNameSuffix2))
  val fileNameParam4: FileNameParam = FileNameParam(Some(fileNamePrefix2), Some(fileNameSuffix2), Some(fileNameSeparator))

  // fixture around data-path
  val dataPath: DataPath = DataPath(pathPrefix = Some(pathPrefix), None, None, None)
  val dataPath1: DataPath = DataPath(pathPrefix = Some(pathPrefix), cataloguePatterns = cataloguePatterns1, None, None)
  val dataPath2: DataPath = DataPath(pathPrefix = Some(pathPrefix), cataloguePatterns = cataloguePatterns1,
    feedPattern = Some(feedPattern1), None)
  val dataPath3: DataPath = DataPath(pathPrefix = Some(pathPrefix), cataloguePatterns = cataloguePatterns1,
    feedPattern = Some(feedPattern1), fileName = Some(fileNameParam1))
}
