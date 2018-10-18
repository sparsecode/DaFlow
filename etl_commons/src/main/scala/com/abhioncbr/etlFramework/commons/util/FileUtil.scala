package com.abhioncbr.etlFramework.commons.util

import java.nio.file.{Path, Paths}
import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.{JOB_STATIC_PARAM, OTHER_PARAM}
import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.common.file.{FileNameParam, FilePath, PathInfixParam}
import com.abhioncbr.etlFramework.commons.job.JobStaticParam
import com.abhioncbr.etlFramework.commons.{Context, ProcessFrequencyEnum}
import org.joda.time.{DateTime, Days, DurationFieldType}

object FileUtil {
  val processFrequency: ProcessFrequencyEnum.frequencyType = Context.getContextualObject[JobStaticParam](JOB_STATIC_PARAM).processFrequency


  def getFilePathString(filePath: FilePath): String = {
    val pathPrefixString = filePath.pathPrefix.getOrElse("")
    val groupsString = getInfixPathString[Array[PathInfixParam]](filePath.groupPatterns)
    val feedString = getInfixPathString[PathInfixParam](filePath.feedPattern)
    val fileNameString = getFileNameString(filePath.fileName)
    s"""$pathPrefixString$groupsString$feedString$fileNameString"""
  }

  def getFilePathObject(filePathString: String): FilePath = {
    val parsedPath: Path = Paths.get(filePathString)
    val pathPrefix = parsedPath.getParent.toString
    FilePath(Some(pathPrefix),
      fileName = Some(parseFileName(parsedPath.getFileName.toString, parsedPath.getFileSystem.getSeparator)))
  }

  private def parseFileName(rawFileName: String, separator: String): FileNameParam ={
    val nameParts: Array[String] = rawFileName.split(separator)
    FileNameParam(Some(nameParts(0)), Some(nameParts(1)), Some(separator))
  }

  private def getFormattedString(pattern: String, args: Option[Array[String]]): String = String.format(pattern, args.get:_*)

  private def getFileNameString(fileNameParamOption: Option[FileNameParam]): String = {
    fileNameParamOption.getOrElse(None) match {
      case fileNameParam: FileNameParam => s"""/${fileNameParam.fileNamePrefix.getOrElse("*")}${fileNameParam.fileNameSeparator.get}${fileNameParam.fileNameSuffix.getOrElse("*")}"""
      case None => ""
    }

  }

  private def getInfixPathString[T](infixObject: Option[T]): String ={
    val output = infixObject.getOrElse(None) match {
      case groups: Array[PathInfixParam] => groups.
        map(gr => if(gr.formatInfix.get) getFormattedString(gr.infixPattern, mapFormatArgs(gr.formatInfixArgs)) else gr.infixPattern).
        mkString("/", "/", "")
      case feed: PathInfixParam => if(feed.formatInfix.get) getFormattedString(feed.infixPattern, mapFormatArgs(feed.formatInfixArgs)) else feed.infixPattern
      case None => ""
    }
    output
  }

  def mapFormatArgs(generalParams: Option[Array[GeneralParam]]): Option[Array[String]] ={
    val programParams = Context.getContextualObject[Option[Map[String,String]]](OTHER_PARAM).get
    generalParams.getOrElse(None) match {
      case None => None
      case params: Array[GeneralParam] => Some(params.map(param => programParams.getOrElse(param.paramName, param.paramValue)))
    }
  }


  private def getProcessFrequencyPattern(firstDate: Option[DateTime], secondDate: Option[DateTime]): String = {
    import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum._
    processFrequency match {
      case ONCE => ""

      case HOURLY  => val parsedDate = processDate(firstDate.get)
        s"""${parsedDate._5}/${parsedDate._4}/${parsedDate._2}/${parsedDate._1}"""

      case DAILY   => val parsedDate = processDate(firstDate.get)
        s"""${parsedDate._5}/${parsedDate._4}/${parsedDate._2}/*"""

      case MONTHLY => val parsedDate = processDate(firstDate.get)
        s"""${parsedDate._5}/${parsedDate._4}/*/*"""

      case YEARLY  => val parsedDate = processDate(firstDate.get)
        s"""${parsedDate._5}/*/*/*"""

      case WEEKLY  => val parsedDate = processDate(firstDate.get)
        val dateString = multipleDatesPattern(parsedDate._3.roundFloorCopy(), parsedDate._3.roundCeilingCopy)
        s"""${parsedDate._5}/${parsedDate._4}/$dateString/*"""

      case DATE_RANGE => val parsedDate = processDate(firstDate.get)
        val dateString = multipleDatesPattern(firstDate.get, secondDate.get)
        s"""${parsedDate._5}/${parsedDate._4}/$dateString/*"""
    }
  }

  private def multipleDatesPattern(firstDate: DateTime, secondDate: DateTime): String ={
    val df = new DecimalFormat("00")
    val days = Days.daysBetween(firstDate, secondDate).getDays
    val dateArray = new Array[DateTime](days)
    for (i <- 0 until days) {
      dateArray(i) = firstDate.withFieldAdded(DurationFieldType.days(), i)
    }
    val dateValues = dateArray.map(date => (date, df.format(date.getDayOfMonth))).toMap.values
    val dateString = if (dateValues.size > 1) dateValues.mkString("{", ",", "}") else dateValues.mkString
    dateString
  }

  private def processDate(date: DateTime) :(String, String, DateTime.Property , String, Int) = {
    val df = new DecimalFormat("00")
    val hour: String = df.format(date.getHourOfDay)
    val day: String = df.format(date.getDayOfMonth)
    val weekOfWeekYear: DateTime.Property = date.weekOfWeekyear
    val month: String = df.format(date.getMonthOfYear)
    val year: Int = date.getYear
    (hour, day, weekOfWeekYear, month, year)
  }
}
