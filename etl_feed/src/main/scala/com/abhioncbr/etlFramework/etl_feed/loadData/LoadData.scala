package com.abhioncbr.etlFramework.etl_feed.loadData

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.job.JobStaticParam
import com.abhioncbr.etlFramework.commons.load.{PartitionColumnTypeEnum, Load}
import com.abhioncbr.etlFramework.commons.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

trait LoadData{
  def loadTransformedData(dataFrame: DataFrame, date: DateTime): Either[Boolean, String]
}

class LoadDataIntoHiveTable extends LoadData {
  private val hiveContext = Context.getContextualObject[HiveContext](HIVE_CONTEXT)

  private val processFrequency = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM).processFrequency

  private val load = Context.getContextualObject[Load](LOAD)
  private val tableName = load.tableName
  private val databaseName = load.dbName
  private val partData = load.partData
  private val hiveTableDataInitialPath = load.partData.partitionFileInitialPath

  def loadTransformedData(dataFrame: DataFrame, date: DateTime): Either[Boolean, String] = {
    var df = dataFrame
    val dateString = date.toString("yyyy-MM-dd")
    val timeString = s"""${new DecimalFormat("00").format(date.getHourOfDay)}"""
    val path = s"$hiveTableDataInitialPath/$databaseName/$tableName/$dateString/$timeString"

    var output = false
    try{
      Logger.log.info(s"Writing $processFrequency dataFrame for table $tableName to HDFS ($path). Total number of data rows saved: ${dataFrame.count}")

      val fileType = load.fileType

      //coalesce the data frame if it is set true in feed xml
      if(partData.coalesce){
        val currentPartitions = dataFrame.rdd.partitions.length
        val numExecutors = hiveContext.sparkContext.getConf.get("spark.executor.instances").toInt
        if (currentPartitions > numExecutors) {
          df = dataFrame.coalesce(partData.coalesceCount)
        }
      }

      fileType match {
        case "PARQUET" => df.write.mode(SaveMode.Overwrite).parquet(path)
        case _ => return Right(s"hive table data save file type is: $fileType")
      }

      val partitioningString = PartitionColumnTypeEnum.getPartitioningString(partData)
      Logger.log.info(s"partitioning string - $partitioningString")

      hiveContext.sql(
        s"""
           |ALTER TABLE $databaseName.$tableName
           |ADD IF NOT EXISTS PARTITION ($partitioningString)
           |LOCATION '$path'
         """.stripMargin)
      output = true

      Logger.log.info(s"Partition at ($path) registered successfully to $databaseName.$tableName")
    }
    Left(output)
  }
}
