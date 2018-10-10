package com.abhioncbr.etlFramework.commons.load

object LoadType extends Enumeration {
  type valueType = Value
  val JDBC, JSON, HIVE = Value

  def getValueType(valueTypeString: String): LoadType.valueType = {
    val valueType = valueTypeString match {
      case "JDBC" => LoadType.JDBC
      case "JSON" => LoadType.JSON
      case "HIVE" => LoadType.HIVE
    }
    valueType
  }

  def getDataValue(valueType: LoadType.valueType):  String= {
    val output = valueType match {
      case JDBC => "JDBC"
      case JSON => "JSON"
      case HIVE => "HIVE"
    }
    output
  }
}
