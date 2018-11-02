package com.abhioncbr.etlFramework.core.loadData

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.{FIRST_DATE, JOB_STATIC_PARAM_CONF, SQL_CONTEXT}
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.abhioncbr.etlFramework.commons.load.LoadFeedConf
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
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
                          date: Option[DateTime]=Context.getContextualObject[Option[DateTime]](FIRST_DATE)): Either[Boolean, String] = {
    var df = dataFrame
    val dateString = date.get.toString("yyyy-MM-dd")
    val timeString = s"""${new DecimalFormat("00").format(date.get.getHourOfDay)}"""
    val path = s"$hiveTableDataInitialPath/$databaseName/$tableName/$dateString/$timeString"

    var output = false
    try{
      logger.info(s"Writing $processFrequency dataFrame for table $tableName to HDFS ($path). Total number of data rows saved: ${dataFrame.count}")

      val fileType = feed.attributesMap("fileType")

      //coalesce the data frame if it is set true in feed xml
      if(partData.coalesce){
        val currentPartitions = dataFrame.rdd.partitions.length
        val numExecutors = sqlContext.sparkContext.getConf.get("spark.executor.instances").toInt
        if (currentPartitions > numExecutors) {
          df = dataFrame.coalesce(partData.coalesceCount)
        }
      }

      fileType match {
        case "PARQUET" => df.write.mode(SaveMode.Overwrite).parquet(path)
        case _ => return Right(s"hive table data save file type is: $fileType")
      }

      val partitioningString = LoadUtil.getPartitioningString(partData)
      logger.info(s"partitioning string - $partitioningString")

      sqlContext.sql(
        s"""
           |ALTER TABLE $databaseName.$tableName
           |ADD IF NOT EXISTS PARTITION ($partitioningString)
           |LOCATION '$path'
         """.stripMargin)
      output = true

      logger.info(s"Partition at ($path) registered successfully to $databaseName.$tableName")
    }
    Left(output)
  }
}
