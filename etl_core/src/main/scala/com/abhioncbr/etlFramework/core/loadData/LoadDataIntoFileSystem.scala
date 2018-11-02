package com.abhioncbr.etlFramework.core.loadData

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.JOB_STATIC_PARAM_CONF
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.abhioncbr.etlFramework.commons.load.LoadFeedConf
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.common.DataPath
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class LoadDataIntoFileSystem(feed: LoadFeedConf) extends LoadData {
  private val logger = Logger(this.getClass)
  private val processFrequency = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF).processFrequency

  private val datasetName: String = feed.attributesMap.getOrElse("catalogName", "")
  private val feedName = feed.attributesMap.getOrElse("feedName", "")
  private val dataPath: DataPath = feed.dataPath

  def loadTransformedData(dataFrame: DataFrame, date: Option[DateTime] = None): Either[Boolean, String] = {
    val path = FileUtil.getFilePathString(dataPath)
    var output = false
    try{
      logger.info(s"Writing $processFrequency dataFrame for feed $feedName to ($path). Total number of data rows saved: ${dataFrame.count}")
      val fileType = feed.attributesMap("fileType")

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
