package com.abhioncbr.etlFramework.commons.extract

object ExtractionType extends Enumeration {
  type valueType = Value
  val JDBC, HIVE, FILE_SYSTEM, UNSUPPORTED  = Value

  def getValueType(valueTypeString: String): ExtractionType.valueType = {
    val valueType = valueTypeString match {
      case "JDBC" => ExtractionType.JDBC
      case "HIVE" => ExtractionType.HIVE
      case "FILESYSTEM" => ExtractionType.FILE_SYSTEM
      case "UNSUPPORTED" => ExtractionType.UNSUPPORTED
    }
    valueType
  }

  def getDataValue(valueType: ExtractionType.valueType):  String= {
    val output = valueType match {
      case JDBC => "JDBC"
      case HIVE => "HIVE"
      case FILE_SYSTEM => "FILE_SYSTEM"
      case UNSUPPORTED => "UNSUPPORTED"
    }
    output
  }
}