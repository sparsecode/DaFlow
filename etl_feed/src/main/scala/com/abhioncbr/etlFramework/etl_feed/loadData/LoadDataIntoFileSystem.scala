package com.abhioncbr.etlFramework.etl_feed.loadData

import java.text.DecimalFormat

import ch.qos.logback.core.rolling.helper.FileNamePattern
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.{JOB_STATIC_PARAM, LOAD}
import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.job.JobStaticParam
import com.abhioncbr.etlFramework.commons.load.Load
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.abhioncbr.etlFramework.commons.Context
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class LoadDataIntoFileSystem extends LoadData {
  private val logger = Logger(this.getClass)
  private val processFrequency = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM).processFrequency

  private val load = Context.getContextualObject[Load](LOAD)
  private val datasetName = load.datasetName
  private val feedName = load.feedName
  private val dataPath: FilePath = load.dataPath

  def loadTransformedData(dataFrame: DataFrame, date: Option[DateTime] = None): Either[Boolean, String] = {
    val path = FileUtil.getFilePathString(dataPath)
    var output = false
    try{
      logger.info(s"Writing $processFrequency dataFrame for feed $feedName to ($path). Total number of data rows saved: ${dataFrame.count}")
      val fileType = load.fileType

      fileType match {
        case "CSV" => dataFrame.write.mode(SaveMode.Overwrite).csv(path)
        case "JSON" => dataFrame.write.mode(SaveMode.Overwrite).json(path)
        case "PARQUET" => dataFrame.write.mode(SaveMode.Overwrite).parquet(path)
        case _ => return Right(s"file type '$fileType' not supported.")
      }
      output = true

      logger.info(s"Data written at ($path) successfully.")
    }
    Left(output)
  }
}
