package com.abhioncbr.etlFramework.commons.extract

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
