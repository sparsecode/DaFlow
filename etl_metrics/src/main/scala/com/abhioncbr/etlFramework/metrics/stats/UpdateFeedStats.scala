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

package com.abhioncbr.etlFramework.metrics.stats

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.JOB_STATIC_PARAM_CONF
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.SPARK_CONTEXT
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.SQL_CONTEXT
import com.abhioncbr.etlFramework.commons.NotificationMessages.{exceptionMessage => EM}
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.typesafe.scalalogging.Logger
import java.io.BufferedWriter
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import scala.util.Try

class UpdateFeedStats(jobName: String, startDate: DateTime = DateTime.now) {
  private val logger = Logger(this.getClass)

  def updateFeedStatInFile(executionTime: Long, feedJobResult: JobResult, filePath: Option[String] = None)
  : Either[Unit, String] = {
    val statString: String = getStatString(executionTime, feedJobResult)
    val defaultPath: String = s"${System.getProperty("user.dir")}/etl_examples/sample_feed_stats/$jobName-stat.csv"
    val path: String = filePath.getOrElse(defaultPath)

    logger.info(s"[updateFeedStatInFile]: writing stat in file: $path.")

    // TODO/FIXME: Make file writer more configurable
    val result: Either[Unit, String] = try {
      val writer: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path, true)))
      writer.write(statString)
      writer.flush()
      writer.close()
      Left()
    } catch {
      case fileNotFoundException: FileNotFoundException => Right(s" ${EM(fileNotFoundException)} ".stripMargin)
      case exception: Exception => Right(s" ${EM(exception)} ".stripMargin)
    }
    result
  }

  def updateFeedStatInHive(hiveDbName: String, tableName: String, pathInitial: String,
    executionTime: Long, feedJobResult: JobResult): Either[Unit, String] = {

    val statString: String = getStatString(executionTime, feedJobResult)
    val jobStaticParam = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF)

    val sc: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
    val rdd = sc.parallelize(Seq(Seq(statString)))
    val rowRdd = rdd.map(v => org.apache.spark.sql.Row(v: _*))

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val statDF = sqlContext.sql(s"select * from $hiveDbName.$tableName where job_name='$jobName'".stripMargin)
    val newRow = sqlContext.createDataFrame(rowRdd, statDF.schema)
    val updatedStatDF = statDF.union(newRow)

    val path = s"$pathInitial/$hiveDbName/$tableName/${jobStaticParam.jobName}/"
    val tmpPath = path + "_tmp"

    val result: Either[Unit, String] = try {
      logger.info(s"[updateFeedStatInHive]: Writing updated stats in to the table '$tableName' to HDFS ($path)")
      updatedStatDF.write.mode(SaveMode.Overwrite).parquet(tmpPath)

      val hadoopFs = FileSystem.get(new Configuration())
      Try(hadoopFs.delete(new Path(path + "_bkp"), true))
      Try(hadoopFs.rename(new Path(path), new Path(path + "_bkp")))
      hadoopFs.rename(new Path(tmpPath), new Path(path))

      sqlContext.sql(
        s" ALTER TABLE $hiveDbName.$tableName ADD IF NOT EXISTS PARTITION (job_name ='$jobName') LOCATION '$path'".stripMargin)

      logger.info(s"[updateFeedStatInHive]: Updated stats partition at ($path) registered successfully to $hiveDbName.$tableName")
      Left()
    } catch {
      case exception: Exception => Right(s" ${EM(exception)} ".stripMargin)
    }
    result
  }

  private def getStatString(executionTime: Long, feedJobResult: JobResult): String = {
    val datePattern = "yyyy-MM-dd"
    val dateString = startDate.toString(datePattern)

    val stat: String = s"$jobName, ${feedJobResult.feedName}, ${feedJobResult.success}, $dateString, ${startDate.hourOfDay}, " +
      s"${feedJobResult.transformationPassedCount}, ${feedJobResult.transformationFailedCount}, " +
      s"${feedJobResult.validateCount}, ${feedJobResult.nonValidatedCount}, ${feedJobResult.failureReason}, $executionTime"
    logger.info(s"Stat Row: $stat")
    stat
  }

}
