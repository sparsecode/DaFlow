package com.abhioncbr.etlFramework.etl_feed.extractData

import com.abhioncbr.etlFramework.commons.extract.{Extract, Feed}
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.Logger
import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.util.FileUtil
import org.apache.spark.sql.{DataFrame, SQLContext}

class ExtractDataFromJson(feed: Feed) extends ExtractData {
   val dataPath: Option[FilePath]= feed.dataPath

  def getRawData: DataFrame = {
    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val fileNamePatternString = FileUtil.getFilePathString(dataPath.get)
    Logger.log.info(fileNamePatternString)
    sqlContext.read.json(fileNamePatternString)
  }
}
