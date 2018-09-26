package com.abhioncbr.etlFramework.etl_feed.extractData

import com.abhioncbr.etlFramework.commons.extract.Extract
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.Logger
import com.abhioncbr.etlFramework.etl_feed.util.FileNamePattern
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime

class ExtractDataFromJson(filePathPrefix: String = Context.getContextualObject[Extract](EXTRACT).fileInitialPath,
                          fileNamePattern: String = Context.getContextualObject[Extract](EXTRACT).fileNamePattern,
                          formatFileName: Boolean = Context.getContextualObject[Extract](EXTRACT).formatFileName,
                          filePrefix: String = Context.getContextualObject[Extract](EXTRACT).filePrefix) extends ExtractData {
  val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)

  def getRawData(firstDate: DateTime, secondDate: Option[DateTime]): DataFrame = {
    val namePattern= new FileNamePattern
    val fileNamePatternString = if(formatFileName) namePattern.getFileDataPathPattern(filePathPrefix, fileNamePattern, filePrefix, firstDate, secondDate)
                                else s"$filePathPrefix/$fileNamePattern"

    Logger.log.info(fileNamePatternString)

    sqlContext.read.json(fileNamePatternString)
  }
}
