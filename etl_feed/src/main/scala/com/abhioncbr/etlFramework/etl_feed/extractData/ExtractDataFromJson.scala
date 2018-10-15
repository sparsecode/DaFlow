package com.abhioncbr.etlFramework.etl_feed.extractData

import com.abhioncbr.etlFramework.commons.extract.Extract
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.Logger
import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.util.FileUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime

class ExtractDataFromJson(dataPath: Option[FilePath] = Context.getContextualObject[Extract](EXTRACT).dataPath,
                          firstDate: Option[DateTime] = Context.getContextualObject[Option[DateTime]](FIRST_DATE),
                          secondDate: Option[DateTime] = Context.getContextualObject[Option[DateTime]](SECOND_DATE)) extends ExtractData {
  val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)

  def getRawData: DataFrame = {
    val fileNamePatternString = FileUtil.getFilePathString(dataPath.get)
    Logger.log.info(fileNamePatternString)
    sqlContext.read.json(fileNamePatternString)
  }
}
