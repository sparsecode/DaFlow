package com.abhioncbr.etlFramework.commons.load

object LoadType extends Enumeration {
  type valueType = Value
  val JDBC, HIVE, FILE_SYSTEM  = Value

  def getValueType(valueTypeString: String): LoadType.valueType = {
    val valueType = valueTypeString match {
      case "JDBC" => LoadType.JDBC
      case "HIVE" => LoadType.HIVE
      case "FILESYSTEM" => LoadType.FILE_SYSTEM
    }
    valueType
  }

  def getDataValue(valueType: LoadType.valueType):  String= {
    val output = valueType match {
      case JDBC => "JDBC"
      case HIVE => "HIVE"
      case FILE_SYSTEM => "FILESYSTEM"
    }
    output
  }
}
