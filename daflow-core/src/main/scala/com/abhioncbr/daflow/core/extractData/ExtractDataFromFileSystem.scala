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
import com.abhioncbr.daflow.commons.NotificationMessages.{extractNotSupported => ENS}
import com.abhioncbr.daflow.commons.conf.common.DataPath
import com.abhioncbr.daflow.commons.conf.extract.ExtractFeedConf
import com.abhioncbr.daflow.commons.util.FileUtil
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SQLContext

class ExtractDataFromFileSystem(feed: ExtractFeedConf) extends ExtractData {
  private val logger = Logger(this.getClass)
  val dataPath: Option[DataPath] = feed.dataPath

  def getRawData: Either[ExecutionResult, String] = {
    try {
      val sqlContext: SQLContext =
        Context.getContextualObject[SQLContext](SQL_CONTEXT)
      val fileNamePatternString = FileUtil.getFilePathString(dataPath.get)
      logger.info(
        s"[ExtractDataFromFileSystem]-[getRawData]: path of data extraction: $fileNamePatternString"
      )

      val output: Either[ExecutionResult, String] =
        feed.extractionAttributesMap("fileType") match {
          case "CSV" =>
            Left(
              ExecutionResult(
                feed.extractFeedName,
                sqlContext.read.csv(fileNamePatternString)
              )
            )
          case "JSON" =>
            Left(
              ExecutionResult(
                feed.extractFeedName,
                sqlContext.read.json(fileNamePatternString)
              )
            )
          case "PARQUET" =>
            Left(
              ExecutionResult(
                feed.extractFeedName,
                sqlContext.read.parquet(fileNamePatternString)
              )
            )
          case _ =>
            Right(
              s"[ExtractDataFromFileSystem]-[getRawData]: ${ENS(feed.extractionAttributesMap("fileType"))}"
            )
        }
      output
    } catch {
      case exception: Exception =>
        logger.error("[ExtractDataFromFileSystem]-[getRawData]: ", exception)
        Right(
          s"[ExtractDataFromFileSystem]-[getRawData]: ${EM(exception)}".stripMargin
        )
    }
  }
}
