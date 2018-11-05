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

package com.abhioncbr.etlFramework.core.loadData

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.JOB_STATIC_PARAM_CONF
import com.abhioncbr.etlFramework.commons.common.DataPath
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.abhioncbr.etlFramework.commons.load.LoadFeedConf
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime

class LoadDataIntoFileSystem(feed: LoadFeedConf) extends LoadData {
  private val logger = Logger(this.getClass)
  private val processFrequency = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF).processFrequency

  private val datasetName: String = feed.attributesMap.getOrElse("catalogName", "")
  private val feedName = feed.attributesMap.getOrElse("feedName", "")
  private val dataPath: DataPath = feed.dataPath

  def loadTransformedData(dataFrame: DataFrame, date: Option[DateTime] = None): Either[Boolean, String] = {
    val path = FileUtil.getFilePathString(dataPath)

    try{
      logger.info(s"Writing $processFrequency dataFrame for dataset: $datasetName, feed $feedName to ($path). " +
        s"Total number of data rows saved: ${dataFrame.count}")

      val fileType = feed.attributesMap("fileType")

      val output: Either[Boolean, String] = fileType match {
        case "CSV" => dataFrame.write.mode(SaveMode.Overwrite).csv(path)
          logger.info(s"Data written at ($path) successfully.")
          Left(true)

        case "JSON" => dataFrame.write.mode(SaveMode.Overwrite).json(path)
          logger.info(s"Data written at ($path) successfully.")
          Left(true)

        case "PARQUET" => dataFrame.write.mode(SaveMode.Overwrite).parquet(path)
          logger.info(s"Data written at ($path) successfully.")
          Left(true)

        case _ => Right(s"file type '$fileType' not supported.")
      }

      output
    }
  }
}
