package com.abhioncbr.etlFramework.etl_feed.loadData

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.{JOB_STATIC_PARAM, LOAD}
import com.abhioncbr.etlFramework.commons.job.JobStaticParam
import com.abhioncbr.etlFramework.commons.load.Load
import com.abhioncbr.etlFramework.commons.{Context, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class LoadDataIntoFileSystem extends LoadData {
  private val processFrequency = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM).processFrequency

  private val load = Context.getContextualObject[Load](LOAD)
  private val datasetName = load.datasetName
  private val feedName = load.feedName
  private val initialPath = load.fileInitialPath

  def loadTransformedData(dataFrame: DataFrame, date: DateTime): Either[Boolean, String] = {
    val dateString = date.toString("yyyy-MM-dd")
    val timeString = s"""${new DecimalFormat("00").format(date.getHourOfDay)}"""
    val path = s"$initialPath/$datasetName/$feedName/$dateString/$timeString"

    var output = false
    try{
      Logger.log.info(s"Writing $processFrequency dataFrame for table $feedName to ($path). Total number of data rows saved: ${dataFrame.count}")
      val fileType = load.fileType

      fileType match {
        case "CSV" => dataFrame.write.mode(SaveMode.Overwrite).csv(path)
        case "JSON" => dataFrame.write.mode(SaveMode.Overwrite).json(path)
        case "PARQUET" => dataFrame.write.mode(SaveMode.Overwrite).parquet(path)
        case _ => return Right(s"file type '$fileType' not supported.")
      }
      output = true

      Logger.log.info(s"Data written at ($path) successfully.")
    }
    Left(output)
  }
}
