package com.abhioncbr.etlFramework.etl_feed.extractData

import com.abhioncbr.etlFramework.commons.extract.Feed
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}

class ExtractDataFromFileSystem(feed: Feed) extends ExtractData {
   private val logger = Logger(this.getClass)
   val dataPath: Option[FilePath]= feed.dataPath

  def getRawData: DataFrame = {
    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val fileNamePatternString = FileUtil.getFilePathString(dataPath.get)
    logger.info(fileNamePatternString)
    sqlContext.read.json(fileNamePatternString)
  }
}
