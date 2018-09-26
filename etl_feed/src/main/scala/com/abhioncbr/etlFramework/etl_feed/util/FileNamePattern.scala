package com.abhioncbr.etlFramework.etl_feed.util

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.JOB_STATIC_PARAM
import com.abhioncbr.etlFramework.commons.{Context, ProcessFrequencyEnum}
import com.abhioncbr.etlFramework.commons.job.JobStaticParam
import org.joda.time.{DateTime, Days, DurationFieldType}

class FileNamePattern {
  val processFrequency: ProcessFrequencyEnum.frequencyType = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM).processFrequency

  def getFileDataPathPattern(filePathPrefix: String, fileNamePattern: String, filePrefix: String, firstDate: DateTime, secondDate: Option[DateTime]): String = {
    s"""$filePathPrefix/${getFeedName(fileNamePattern)}/${getProcessFrequencyPattern(filePrefix, firstDate,secondDate)}"""
  }

  private def getFeedName(namePattern: String): String = String.format(namePattern)
  private def getProcessFrequencyPattern(filePrefix: String, firstDate: DateTime, secondDate: Option[DateTime]): String = {
    val df = new DecimalFormat("00")
    import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum._

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
