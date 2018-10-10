package com.abhioncbr.etlFramework.commons.load

object LoadType extends Enumeration {
  type valueType = Value
  val JDBC, FILE_SYSTEM, HIVE = Value

  def getValueType(valueTypeString: String): LoadType.valueType = {
    val valueType = valueTypeString match {
      case "JDBC" => LoadType.JDBC
      case "FILE_SYSTEM" => LoadType.FILE_SYSTEM
      case "HIVE" => LoadType.HIVE
    }
    valueType
  }

  def getDataValue(valueType: LoadType.valueType):  String= {
    val output = valueType match {
      case JDBC => "JDBC"
      case FILE_SYSTEM => "FILE_SYSTEM"
      case HIVE => "HIVE"
    }
    output
  }
}
