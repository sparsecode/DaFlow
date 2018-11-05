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

package com.abhioncbr.etlFramework.core

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.extract.ExtractConf
import com.abhioncbr.etlFramework.commons.extract.ExtractionType
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.abhioncbr.etlFramework.commons.load.{LoadConf, LoadType}
import com.abhioncbr.etlFramework.commons.transform.{TransformConf, TransformResult}
import com.abhioncbr.etlFramework.core.extractData.{ExtractDataFromDB, ExtractDataFromFileSystem, ExtractDataFromHive}
import com.abhioncbr.etlFramework.core.loadData.{LoadDataIntoFileSystem, LoadDataIntoHive}
import com.google.common.base.Objects
import com.abhioncbr.etlFramework.core.transformData.{Transform, TransformData, TransformUtil}
import com.abhioncbr.etlFramework.metrics.stats.UpdateFeedStats
import com.abhioncbr.etlFramework.jobConf.xml.ParseETLJobXml
import org.apache.hadoop.conf.Configuration
import com.abhioncbr.etlFramework.metrics.stats.JobResult
import com.abhioncbr.etlFramework.core.validateData.ValidateTransformedData
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.xml.XML

class LaunchETLSparkJobExecution(feedName: String ,firstDate: Option[DateTime], secondDate: Option[DateTime],
                                 xmlInputFilePath: String, otherParams: Option[Map[String, String]]){
  private val logger = Logger(this.getClass)

  def configureFeedJob: Either[Boolean, String] ={
    Context.addContextualObject[Option[DateTime]](FIRST_DATE, firstDate)
    Context.addContextualObject[Option[DateTime]](SECOND_DATE, secondDate)

    val appName = s"etl-$feedName"
    val sparkSession: SparkSession = SparkSession.builder().appName(appName).getOrCreate()
    Context.addContextualObject[Option[Map[String,String]]](OTHER_PARAM, otherParams)
    Context.addContextualObject[Configuration](HADOOP_CONF, sparkSession.sparkContext.hadoopConfiguration)
    Context.addContextualObject[SparkContext](SPARK_CONTEXT, sparkSession.sparkContext)
    Context.addContextualObject[SQLContext](SQL_CONTEXT, sparkSession.sqlContext)

    val FALSE = false
    val parse = new ParseETLJobXml
    parse.parseXml(xmlInputFilePath, FALSE) match {
      case Left(xmlContent) => parse.parseNode(XML.loadString(xmlContent)) match {
        case Left(tuple) =>
          Context.addContextualObject[JobStaticParamConf] (JOB_STATIC_PARAM_CONF, tuple._1)
          Context.addContextualObject[ExtractConf] (EXTRACT_CONF, tuple._2)
          Context.addContextualObject[TransformConf] (TRANSFORM_CONF, tuple._3)
          Context.addContextualObject[LoadConf] (LOAD_CONF, tuple._4)
        case Right(parseError) => logger.error(parseError)
          return Right(parseError)
      }
      case Right(xmlLoadError) => logger.error(xmlLoadError)
        return Right(xmlLoadError)
    }

    Left(true)
  }

  def executeFeedJob: Either[Array[JobResult], String]={
    val extractionResult = extract
    val extractionRight: Array[String] = extractionResult.filter(_.isRight).map(_.right.get)
    if(extractionRight.length > 0) return Right(extractionRight.mkString(" , "))
    logger.info("Extraction phase of the feed is completed")

    //TODO: validate extracted data based on condition & boolean operator.

    val transformedResult = transformation(extractionResult.map(_.left.get))
    if(transformedResult.isRight) return Right(transformedResult.right.get)
    logger.info("Transformation phase of the feed is completed")

    // validating transformed data, if it is configured to be validated.
    val validateTransformedData: Boolean  = Context.getContextualObject[TransformConf](TRANSFORM_CONF).validateTransformedData
    val validateResult: Either[Array[(DataFrame, DataFrame, Any, Any)], String]  = if(validateTransformedData) {
      validate(transformedResult.left.get)
    } else {
      Left(transformedResult.left.get.map(array => (array.resultDF, null, array.otherAttributes.get("validCount"),
        array.otherAttributes.get("invalidCount"))))
    }
    if(validateResult.isRight) return Right(validateResult.right.get)
    if(validateTransformedData) logger.info("Validation phase of the feed is completed")

    // loading the transformed data.
    val loadResult = load(validateResult.left.get)
    if(loadResult.isRight) return Right(validateResult.right.get)
    logger.info ("Load phase of the feed is completed")

    Left(loadResult.left.get)
  }

  private def extract: Array[Either[DataFrame, String]] = {
    val extract: ExtractConf = Context.getContextualObject[ExtractConf](EXTRACT_CONF)
    extract.feeds.map(feed => feed.extractionType match {
      case ExtractionType.FILE_SYSTEM => Left(new ExtractDataFromFileSystem(feed).getRawData)
      case ExtractionType.JDBC => Left(new ExtractDataFromDB(feed).getRawData)
      case ExtractionType.HIVE => Left(new ExtractDataFromHive(feed).getRawData)
      case ExtractionType.UNSUPPORTED => Right(s"extracting data from ${feed.extractionType} is not supported right now.")
    })
  }

  private def transformation(extractionDF: Array[DataFrame]): Either[Array[TransformResult], String] = {
    //testing whether extracted data frame is having data or not. If not then, error message is returned.
    if(extractionDF.head.first == null) return Right("Extracted data frame contains no data row")
    val transform: Transform  = TransformUtil.prepareTransformation(Context.getContextualObject[TransformConf](TRANSFORM_CONF))
    new TransformData(transform).performTransformation(extractionDF)
  }

  private def validate(transformationDF: Array[TransformResult]): Either[Array[(DataFrame, DataFrame, Any, Any)], String] = {
    //testing whether transformed data frames have data or not.
    transformationDF.map(res => res.resultDF).foreach(df => if(df.first == null ) return Right("Transformed data frame contains no data row"))

    var output: Array[ (DataFrame, DataFrame, Any, Any) ] = Array()
    transformationDF.foreach( arrayElement =>  {
      val validator = new ValidateTransformedData
      val validateSchemaResult = validator.validateSchema (arrayElement.resultDF)
      if(validateSchemaResult._1) {
        output = output ++ validator.validateData(arrayElement.resultDF, validateSchemaResult._2.get, arrayElement
          .otherAttributes.get("validCount"), arrayElement.otherAttributes.get("invalidCount"))
      } else { validateSchemaResult._2 match {
          case Some(_2) => logger.error("hive table schema & data frame schema does not match. Below are schemas for reference -")
            logger.error(s"table schema:: ${_2.mkString}")
            logger.error(s"data frame schema:: ${validateSchemaResult._3.get.mkString}")
          case None => logger.error(s"provided hive table does not exist.")}
          return Right("Validation failed. Please check the log.")
      }
    })
    Left(output)
  }

  private def load(validationArrayDF: Array[ (DataFrame, DataFrame, Any, Any) ]): Either[Array[JobResult], String] = {
    //testing whether validated data frames have data or not.
    validationArrayDF.foreach(tuple => {
      if (tuple._1.first == null) {
        println(s"validate failed"); return Right("Transformed data frame contains no data row")
      }
    })

    val FALSE = false
    //TODO: load tables for multiple data frames
    val loadResult: Array[JobResult] = validationArrayDF.map( validate => {
      val loadConf = Context.getContextualObject[LoadConf](LOAD_CONF)

      //TODO: handle mapping of the transform feeds with load feeds.
      val feed = loadConf.feeds.head
      val loadType = feed.loadType

      val loadResult: Either[Boolean, String] = loadType match {
        case LoadType.HIVE => new LoadDataIntoHive(feed).loadTransformedData(validate._1)
        case LoadType.JDBC => Right(s"loading data to $loadType is not supported right now.")
        case LoadType.FILE_SYSTEM => new LoadDataIntoFileSystem(feed).loadTransformedData(validate._1)
        case _ => Right(s"loading data to $loadType is not supported right now.")
      }
      //writing output data tuple.
      (loadResult, validate._1.count(), if(validate._2 != null ) validate._2.count() else 0, validate._3, validate._4)
    }).map(result => {
      if(result._1.isRight) JobResult(FALSE, "", result._4.asInstanceOf[Int], result._5.asInstanceOf[Int], result._2, result._3, result._1.right.get)
      else JobResult(result._1.left.get, "", result._4.asInstanceOf[Int], result._5.asInstanceOf[Int], result._2, result._3, "")})
    Left(loadResult)
  }


}

object LaunchETLSparkJobExecution extends App{
  private val logger = Logger(this.getClass)
  def launch(args: Array[String]): Unit = {
    case class CommandOptions(firstDate: Option[DateTime] = None, feedName : String = "", secondDate: Option[DateTime] = None,
                              xmlInputFilePath: String = "", statScriptFilePath: String = "",
                              otherParams: Option[Map[String, String]] = None) {
      override def toString: String =
        Objects.toStringHelper(this)
          .add("feed_name", feedName)
          .add("firstDate", firstDate)
          .add("secondDate", secondDate)
          .add("xmlFilePath", xmlInputFilePath)
          .add("statScriptFilePath", statScriptFilePath)
          .add("otherParams", otherParams)
          .toString
    }
    val DatePattern = "yyyy-MM-dd HH:mm:ss"
    val dateParser  = DateTimeFormat.forPattern(DatePattern)

    val parser = new scopt.OptionParser[CommandOptions]("") {
      opt[String]('e', "etl_feed_name")
        .action((e, f) => f.copy(feedName = e))
        .text("Required Etl feed name.")
        .required

      opt[String]('d', "date")
        .action((fd, c) =>  c.copy(firstDate = if(!fd.trim.isEmpty) Some(dateParser.parseDateTime(fd)) else None))
        .text("Required for all jobs except of hourly jobs or once execution jobs.")
        .optional

      opt[String]('s', "second_date")
        .action((sd, c) =>  c.copy(secondDate = if(!sd.trim.isEmpty) Some(dateParser.parseDateTime(sd)) else None))
        .text("Required only in case of date range jobs")
        .optional

      opt[String]('x', "xml_file_path")
        .action((xfp, c) => c.copy(xmlInputFilePath = xfp))
        .text("Required xml file path for etl job execution steps.")
        .required

      opt[String]('f', "stat_script_file_path")
        .action((sfp, c) => c.copy(statScriptFilePath = sfp))
        .text("Required stat script file path for etl job result updates.")
        .optional

      opt[String]('o', "other_params")
        .action((op, c) => c.copy(otherParams = Some( op.replace("[", "").
          replace("]", "").split(",")
          .map(str => {val temp = str.split("=")
          (temp(0), temp(1))}).toMap)))
        .text("Required in-case job requires extra params like mentioned in 'xml' file. Should be of key-value pattern like '[a=b,x=y]'")
        .optional

    }

    parser.parse(args, CommandOptions()) match {
      case Some(opts) =>
        logger.info(s"Going to start the execution of the etl feed job: ")
        var exitCode = -1
        val etlExecutor = new LaunchETLSparkJobExecution(opts.feedName, opts.firstDate, opts.secondDate, opts.xmlInputFilePath, opts.otherParams)

        var metricData = 0L
        val updateFeedStats: UpdateFeedStats = new UpdateFeedStats(opts.feedName, opts.firstDate.getOrElse(DateTime.now))
        etlExecutor.configureFeedJob match {
          case Left(b) => val start = System.currentTimeMillis()
            val feedJobOutput = etlExecutor.executeFeedJob
            val end = System.currentTimeMillis()
            feedJobOutput match {
              case Left(dataArray) => /*val temp = dataArray.map(data =>
                updateFeedStats.updateFeedStat(opts.statScriptFilePath, data.subtask, data.validateCount,
                  data.nonValidatedCount, end - start, "success", data.transformationPassedCount,
                  data.transformationFailedCount, data.failureReason)).map(status => if(status==0) true else false)
                .reduce(_ && _)
                if(temp) exitCode = 0
                metricData = dataArray.head.validateCount*/
                println("job complete.")
              //else
              case Right(s) => updateFeedStats.updateFeedStat(opts.statScriptFilePath, "", 0, 0, end - start, "fail", 0, 0, s)
                logger.error(s)
            }
          case Right(s) => updateFeedStats.updateFeedStat(opts.statScriptFilePath, "", 0, 0, 0, "fail", 0, 0, s)
            logger.error(s)
        }

        //TODO: Promethus push metrics, commented in refactoring [will be triggered based on job static param]
        //pushMetrics(opts.feedName)

        logger.info(s"Etl job finish with exit code: $exitCode")
        System.exit(exitCode)
      case None =>
        parser.showTryHelp()
    }
  }

  launch(args)
}