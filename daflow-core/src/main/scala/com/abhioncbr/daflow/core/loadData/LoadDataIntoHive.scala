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

package com.abhioncbr.daflow.core.loadData

import java.text.DecimalFormat

import com.abhioncbr.daflow.commons.load.LoadFeedConf
import com.abhioncbr.daflow.commons.util.FileUtil
import com.abhioncbr.daflow.commons.Context
import com.abhioncbr.daflow.commons.job.JobStaticParamConf
import com.abhioncbr.daflow.commons.Context
import com.abhioncbr.daflow.commons.ContextConstantEnum.JOB_STATIC_PARAM_CONF
import com.abhioncbr.daflow.commons.ContextConstantEnum.SQL_CONTEXT
import com.abhioncbr.daflow.commons.ContextConstantEnum.START_DATE
import com.abhioncbr.daflow.commons.job.JobStaticParamConf
import com.abhioncbr.daflow.commons.load.LoadFeedConf
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime

class LoadDataIntoHive(feed: LoadFeedConf) extends LoadData {
  private val logger = Logger(this.getClass)
  private val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)

  private val processFrequency = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF).processFrequency

  private val tableName = feed.attributesMap("tableName")
  private val databaseName = feed.attributesMap("databaseName")
  private val partData = feed.partitioningData.get
  private val hiveTableDataInitialPath = FileUtil.getFilePathString(feed.dataPath)

  def loadTransformedData(dataFrame: DataFrame,
    date: Option[DateTime] = Context.getContextualObject[Option[DateTime]](START_DATE)): Either[Boolean, String] = {
    val dateString = date.get.toString("yyyy-MM-dd")
    val timeString = s"""${new DecimalFormat("00").format(date.get.getHourOfDay)}"""
    val path = s"$hiveTableDataInitialPath/$databaseName/$tableName/$dateString/$timeString"

    try{

      logger.info(s"Writing $processFrequency dataFrame for table $tableName to HDFS ($path). " +
        s"Total number of data rows saved: ${dataFrame.count}")

      val fileType = feed.attributesMap("fileType")

      // coalesce the data frame if it is set true in feed xml
      val df: DataFrame = if (partData.coalesce){
        val currentPartitions = dataFrame.rdd.partitions.length
        val numExecutors = sqlContext.sparkContext.getConf.get("spark.executor.instances").toInt
        if (currentPartitions > numExecutors) { dataFrame.coalesce(partData.coalesceCount) } else { dataFrame }
      } else { dataFrame }

      val output: Either[Boolean, String] = fileType match {
        case "PARQUET" => df.write.mode(SaveMode.Overwrite).parquet(path)
          writeHiveData(path)
          Left(true)
        case _ => Right(s"hive table data save file type is: $fileType")
      }
      output
    }
  }

  private def writeHiveData(path: String): Unit = {
    val partitioningString = LoadUtil.getPartitioningString(partData)
    logger.info(s"partitioning string - $partitioningString")

    sqlContext.sql(s"""
      |ALTER TABLE $databaseName.$tableName
      |ADD IF NOT EXISTS PARTITION ($partitioningString)
      |LOCATION '$path'""".stripMargin)
    logger.info(s"Partition at ($path) registered successfully to $databaseName.$tableName")
  }
}
