package com.lzd.etlFramework.etl.feed.extractData

import java.text.DecimalFormat

import com.lzd.etlFramework.etl.feed.common.ContextConstantEnum._
import com.lzd.etlFramework.etl.feed._
import com.lzd.etlFramework.etl.feed.common.{Context, Extract, JobStaticParam}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.{DateTime, Days, DurationFieldType}

class FileNamePattern {
  val venture = Context.getContextualObject[String](VENTURE)
  val processFrequency = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM).processFrequency

  def getFileDataPathPattern(filePathPrefix: String, fileNamePattern: String, firstDate: DateTime, secondDate: Option[DateTime]) = {
    s"""$filePathPrefix/${getFeedName(fileNamePattern)}/${getProcessFrequencyPattern(firstDate,secondDate)}"""
  }

  private def getFeedName(namePattern: String) = String.format(namePattern, venture)
  private def getProcessFrequencyPattern(firstDate: DateTime, secondDate: Option[DateTime]): String = {
    val df = new DecimalFormat("00")
    import com.lzd.etlFramework.etl.feed.common.ProcessFrequencyEnum._

    val filePrefix = "stream*"
    val year =firstDate.getYear
    val month = df.format(firstDate.getMonthOfYear)
    val day = df.format(firstDate.getDayOfMonth)
    processFrequency match {
      case HOURLY  => val hour = df.format(firstDate.getHourOfDay)
                      s"""$year/$month/$day/$hour/$filePrefix"""

      case DAILY   => s"""$year/$month/$day/*/$filePrefix"""

      case MONTHLY => s"""$year/$month/*/*/$filePrefix"""

      case YEARLY  => s"""$year/*/*/*/$filePrefix"""

      case WEEKLY  => val dateString = multipleDatesPattern(df, firstDate.weekOfWeekyear.roundFloorCopy(), firstDate.weekOfWeekyear.roundCeilingCopy)
                      s"""$year/$month/$dateString/*/$filePrefix"""

      case DATE_RANGE => val dateString = multipleDatesPattern(df, firstDate, secondDate.get)
                      s"""$year/$month/$dateString/*/$filePrefix"""
    }
  }

  private def multipleDatesPattern(df: DecimalFormat, firstDate: DateTime, secondDate: DateTime): String ={
    val days = Days.daysBetween(firstDate, secondDate).getDays
    val dateArray = new Array[DateTime](days)
    for (i <- 0 until days) {
      dateArray(i) = firstDate.withFieldAdded(DurationFieldType.days(), i)
    }
    val dateValues = dateArray.map(date => (date, df.format(date.getDayOfMonth))).toMap.values
    val dateString = if (dateValues.size > 1) dateValues.mkString("{", ",", "}") else dateValues.mkString
    dateString
  }
}

class ExtractDataFromJson(filePathPrefix: String = Context.getContextualObject[Extract](EXTRACT).fileInitialPath,
                          fileNamePattern: String = Context.getContextualObject[Extract](EXTRACT).fileNamePattern,
                          formatFileName: Boolean = Context.getContextualObject[Extract](EXTRACT).formatFileName) extends ExtractData {
  val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)

  def getRawData(firstDate: DateTime, secondDate: Option[DateTime]): DataFrame = {
    val namePattern= new FileNamePattern
    val fileNamePatternString = if(formatFileName) namePattern.getFileDataPathPattern(filePathPrefix, fileNamePattern, firstDate, secondDate)
                                else s"$filePathPrefix/$fileNamePattern"

    Logger.log.info(fileNamePatternString)

    sqlContext.read.json(fileNamePatternString)
  }
}
