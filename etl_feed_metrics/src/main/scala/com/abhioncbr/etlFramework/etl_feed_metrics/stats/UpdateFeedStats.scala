package com.abhioncbr.etlFramework.etl_feed_metrics.stats

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.{HIVE_CONTEXT, JOB_STATIC_PARAM, SPARK_CONTEXT, SQL_CONTEXT}
import com.abhioncbr.etlFramework.commons.job.JobStaticParam
import com.abhioncbr.etlFramework.commons.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.util.Try

class UpdateFeedStats(feed_name: String, firstDate: DateTime) {

  //function for updating etl_feed_stat table through shell script. For airflow-docker image usage, we are going to use other function.
  @deprecated
  def updateFeedStat(statScriptPath: String, validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM)
    val DatePattern = "yyyy-MM-dd"

    val reason = if (failureReason.isEmpty) "None" else s""" "${failureReason.split("[ ]").mkString("_")}" """.trim

    val shellCommand =
      s"""sh $statScriptPath ${jobStaticParam.feedName} $feed_name $status ${jobStaticParam.processFrequency.toString.toLowerCase}
         |   ${firstDate.toString(DatePattern)} ${new DecimalFormat("00").format(firstDate.getHourOfDay)}
         |   $validated $nonValidated $executionTime $transformed $nonTransformed $reason""".stripMargin
    Logger.log.info(s"""going to execute command: $shellCommand""")

    import sys.process._
    shellCommand.!
  }

  def updateFeedStat(statScriptPath: String, jobSubtask: String, validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM)
    val DatePattern = "yyyy-MM-dd"

    val reason = if (failureReason.isEmpty) "None" else s""" "${failureReason.split("[ ]").mkString("_")}" """.trim
    val subTask = if (jobSubtask.isEmpty) jobStaticParam.feedName else jobSubtask

    val shellCommand =
      s"""sh $statScriptPath ${jobStaticParam.feedName} $subTask $status ${jobStaticParam.processFrequency.toString.toLowerCase}
         |   ${firstDate.toString(DatePattern)} ${new DecimalFormat("00").format(firstDate.getHourOfDay)}
         |   $validated $nonValidated $executionTime $transformed $nonTransformed $reason""".stripMargin
    Logger.log.info(s"""going to execute command: $shellCommand""")

    import sys.process._
    shellCommand.!
  }

  def updateFeedStat(hiveDbName: String, tableName: String, pathInitial: String,
                     validated: Long, nonValidated: Long, executionTime: Long,
                     status: String, transformed: Long, nonTransformed: Long, failureReason: String): Int ={
    val jobStaticParam = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM)

    val reason = if (failureReason.isEmpty) "None" else failureReason.trim

    val sc: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
    val rdd = sc.parallelize(Seq(Seq(feed_name, status, jobStaticParam.processFrequency.toString.toLowerCase, new java.sql.Date(firstDate.getMillis), new DecimalFormat("00").format(firstDate.getHourOfDay),
      reason, transformed , nonTransformed, validated, nonValidated, executionTime, jobStaticParam.feedName)))
    val rowRdd = rdd.map(v => org.apache.spark.sql.Row(v: _*))

    val hiveContext: HiveContext = Context.getContextualObject[HiveContext](HIVE_CONTEXT)
    val statDF = hiveContext.sql(s"""select * from $hiveDbName.$tableName where job_name='${jobStaticParam.feedName}'""".stripMargin)

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val newRow = sqlContext.createDataFrame(rowRdd, statDF.schema)
    val updatedStatDF = statDF.unionAll(newRow)

    val path = s"$pathInitial/$hiveDbName/$tableName/${jobStaticParam.feedName}/"
    val tmpPath = path + "_tmp"

    try{
      println(s"Writing updated stats in to the table 'etl_feed_stat' to HDFS ($path)")
      updatedStatDF.write.mode(SaveMode.Overwrite).parquet(tmpPath)

      val hadoopFs = FileSystem.get(new Configuration())
      Try(hadoopFs.delete(new Path(path + "_bkp"), true))
      Try(hadoopFs.rename(new Path(path), new Path(path + "_bkp")))
      hadoopFs.rename(new Path(tmpPath), new Path(path))

      hiveContext.sql(
        s"""
           |ALTER TABLE $hiveDbName.$tableName
           | ADD IF NOT EXISTS PARTITION (job_name ='${jobStaticParam.feedName}')
           | LOCATION '$path'
         """.stripMargin)

      println(s"updated stats partition at ($path) registered successfully to $hiveDbName.$tableName")
      0
    } finally -1
  }

}
