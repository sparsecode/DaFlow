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

package com.abhioncbr.daflow.core.extractData

import com.abhioncbr.daflow.commons.Context
import com.abhioncbr.daflow.commons.ContextConstantEnum._
import com.abhioncbr.daflow.commons.ExecutionResult
import com.abhioncbr.daflow.commons.NotificationMessages.{exceptionMessage => EM}
import com.abhioncbr.daflow.commons.conf.common.GeneralParamConf
import com.abhioncbr.daflow.commons.conf.common.QueryConf
import com.abhioncbr.daflow.commons.conf.extract.ExtractFeedConf
import com.abhioncbr.daflow.commons.util.FileUtil
import com.typesafe.scalalogging.Logger
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class ExtractDataFromHive(feed: ExtractFeedConf) extends AbstractExtractData {
  private val logger = Logger(this.getClass)
  val query: Option[QueryConf] = feed.query

  def getRawData: Either[ExecutionResult, String] = {
    try {
      lazy val fs = FileSystem.get(new Configuration())

      // reading query from the query file.
      val sqlQueryFile: String =
        FileUtil.getFilePathString(query.get.queryFile.queryFile.get)
      val tableQueryReader = new BufferedReader(
        new InputStreamReader(fs.open(new Path(sqlQueryFile)))
      )
      val rawQuery = Stream
        .continually(tableQueryReader.readLine())
        .takeWhile(_ != null)
        .toArray[String]
        .mkString
        .stripMargin

      val sqlQueryParams: Array[GeneralParamConf] = query.get.queryArgs.get
      val queryParams = ExtractUtil.getParamsValue(sqlQueryParams.toList)
      logger.info(
        "[ExtractDataFromHive]-[getRawData]: Qquery param values" + queryParams
          .mkString(" , ")
      )
      val tableQuery = String.format(rawQuery, queryParams: _*)
      logger.info(
        s"[ExtractDataFromHive]-[getRawData]: Going to execute hive query: \\n $tableQuery"
      )

      val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
      val dataFrame: DataFrame = sqlContext.sql(tableQuery)
      Left(ExecutionResult(feed.extractFeedName, dataFrame))
    } catch {
      case exception: Exception =>
        logger.error("[ExtractDataFromHive]-[getRawData]: ", exception)
        Right(
          s"[ExtractDataFromHive]-[getRawData]: ${EM(exception)}".stripMargin
        )
    }
  }
}
