package com.abhioncbr.etlFramework.commons.extract

object ExtractionType extends Enumeration {
  type valueType = Value
  val DB, FILE_SYSTEM, HIVE, UNSUPPORTED  = Value

  def getValueType(valueTypeString: String): ExtractionType.valueType = {
    val valueType = valueTypeString match {
      case "DB" => ExtractionType.DB
      case "HIVE" => ExtractionType.HIVE
      case "FILE_SYSTEM" => ExtractionType.FILE_SYSTEM
      case "UNSUPPORTED" => ExtractionType.UNSUPPORTED
    }
    valueType
  }

  def getDataValue(valueType: ExtractionType.valueType):  String= {
    val output = valueType match {
      case DB => "DB"
      case HIVE => "HIVE"
      case FILE_SYSTEM => "FILE_SYSTEM"
      case UNSUPPORTED => "UNSUPPORTED"
    }
    output
  }
}
