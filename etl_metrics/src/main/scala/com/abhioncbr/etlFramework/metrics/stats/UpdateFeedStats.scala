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

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.{JOB_STATIC_PARAM_CONF, SPARK_CONTEXT, SQL_CONTEXT}
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.joda.time.DateTime

import scala.util.Try

class UpdateFeedStats(feed_name: String, firstDate: DateTime = DateTime.now) {
  private val logger = Logger(this.getClass)

  //function for updating etl_feed_stat table through shell script. For airflow-docker image usage, we are going to use other function.
  @deprecated
  def updateFeedStat(statScriptPath: String, validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF)
    val DatePattern = "yyyy-MM-dd"

    val reason = if (failureReason.isEmpty) "None" else s""" "${failureReason.split("[ ]").mkString("_")}" """.trim

    val shellCommand =
      s"""sh $statScriptPath ${jobStaticParam.jobName} $feed_name $status ${jobStaticParam.processFrequency.toString.toLowerCase}
         |   ${firstDate.toString(DatePattern)} ${new DecimalFormat("00").format(firstDate.getHourOfDay)}
         |   $validated $nonValidated $executionTime $transformed $nonTransformed $reason""".stripMargin
    logger.info(s"""going to execute command: $shellCommand""")

    import sys.process._
    shellCommand.!
  }

  def updateFeedStat(statScriptPath: String, jobSubtask: String, validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF)
    val DatePattern = "yyyy-MM-dd"

    val reason = if (failureReason.isEmpty) "None" else s""" "${failureReason.split("[ ]").mkString("_")}" """.trim
    val subTask = if (jobSubtask.isEmpty) jobStaticParam.jobName else jobSubtask

    val shellCommand =
      s"""sh $statScriptPath ${jobStaticParam.jobName} $subTask $status ${jobStaticParam.processFrequency.toString.toLowerCase}
         |   ${firstDate.toString(DatePattern)} ${new DecimalFormat("00").format(firstDate.getHourOfDay)}
         |   $validated $nonValidated $executionTime $transformed $nonTransformed $reason""".stripMargin
    logger.info(s"""going to execute command: $shellCommand""")

    import sys.process._
    shellCommand.!
  }

  def updateFeedStat(hiveDbName: String, tableName: String, pathInitial: String,
                     validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF)

    val reason = if (failureReason.isEmpty) "None" else failureReason.trim

    val sc: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
    val rdd = sc.parallelize(Seq(Seq(feed_name, status, jobStaticParam.processFrequency.toString.toLowerCase, new java.sql.Date(firstDate.getMillis), new DecimalFormat("00").format(firstDate.getHourOfDay),
      reason, transformed , nonTransformed, validated, nonValidated, executionTime, jobStaticParam.jobName)))
    val rowRdd = rdd.map(v => org.apache.spark.sql.Row(v: _*))

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val statDF = sqlContext.sql(s"""select * from $hiveDbName.$tableName where job_name='${jobStaticParam.jobName}'""".stripMargin)
    val newRow = sqlContext.createDataFrame(rowRdd, statDF.schema)
    val updatedStatDF = statDF.union(newRow)

    val path = s"$pathInitial/$hiveDbName/$tableName/${jobStaticParam.jobName}/"
    val tmpPath = path + "_tmp"

    try{
      println(s"Writing updated stats in to the table 'etl_feed_stat' to HDFS ($path)")
      updatedStatDF.write.mode(SaveMode.Overwrite).parquet(tmpPath)

      val hadoopFs = FileSystem.get(new Configuration())
      Try(hadoopFs.delete(new Path(path + "_bkp"), true))
      Try(hadoopFs.rename(new Path(path), new Path(path + "_bkp")))
      hadoopFs.rename(new Path(tmpPath), new Path(path))

      sqlContext.sql(
        s"""
           |ALTER TABLE $hiveDbName.$tableName
           | ADD IF NOT EXISTS PARTITION (job_name ='${jobStaticParam.jobName}')
           | LOCATION '$path'
         """.stripMargin)

      println(s"updated stats partition at ($path) registered successfully to $hiveDbName.$tableName")
      0
    } finally -1
  }

}
