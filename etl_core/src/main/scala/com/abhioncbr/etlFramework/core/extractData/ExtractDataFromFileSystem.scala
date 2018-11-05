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

package com.abhioncbr.etlFramework.core.extractData

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.common.DataPath
import com.abhioncbr.etlFramework.commons.extract.ExtractFeedConf
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class ExtractDataFromFileSystem(feed: ExtractFeedConf) extends ExtractData {
  private val logger = Logger(this.getClass)
  val dataPath: Option[DataPath] = feed.dataPath

  def getRawData: DataFrame = {
    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val fileNamePatternString = FileUtil.getFilePathString(dataPath.get)
    logger.info(fileNamePatternString)
    sqlContext.read.json(fileNamePatternString)
  }
}
