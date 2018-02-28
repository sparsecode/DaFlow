package com.lzd.etlFramework.etl.feed.extractData

import com.lzd.etlFramework.etl.feed.common.{Context, ContextConstantEnum, QueryParam}
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

/**
  * Created by Abhishek on 22/2/17.
  */
trait ExtractData { def getRawData(firstDate: DateTime, secondDate: Option[DateTime]): DataFrame }

object ExtractionType extends Enumeration {
  type valueType = Value
  val JDBC, JSON, HIVE = Value

  def getValueType(valueTypeString: String): ExtractionType.valueType = {
    val valueType = valueTypeString match {
      case "JDBC" => ExtractionType.JDBC
      case "JSON" => ExtractionType.JSON
      case "HIVE" => ExtractionType.HIVE
    }
    valueType
  }

  def getDataValue(valueType: ExtractionType.valueType):  String= {
    val output = valueType match {
      case JDBC => "JDBC"
      case JSON => "JSON"
      case HIVE => "HIVE"
     }
    output
  }
}
