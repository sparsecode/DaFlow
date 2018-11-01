package com.abhioncbr.etlFramework.core.loadData

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.{JOB_STATIC_PARAM_CONF, LOAD_CONF}
import com.abhioncbr.etlFramework.commons.common.file.DataPath
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.abhioncbr.etlFramework.commons.load.LoadFeedConf
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.abhioncbr.etlFramework.commons.Context
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

class LoadDataIntoFileSystem extends LoadData {
  private val logger = Logger(this.getClass)
  private val processFrequency = Context.getContextualObject[JobStaticParamConf](JOB_STATIC_PARAM_CONF).processFrequency

  private val load = Context.getContextualObject[LoadFeedConf](LOAD_CONF)
  private val datasetName: String = load.attributesMap.getOrElse("catalogName", "")
  private val feedName = load.attributesMap.getOrElse("feedName", "")
  private val dataPath: DataPath = load.dataPath

  def loadTransformedData(dataFrame: DataFrame, date: Option[DateTime] = None): Either[Boolean, String] = {
    val path = FileUtil.getFilePathString(dataPath)
    var output = false
    try{
      logger.info(s"Writing $processFrequency dataFrame for feed $feedName to ($path). Total number of data rows saved: ${dataFrame.count}")
      val fileType = load.attributesMap("fileType")

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
