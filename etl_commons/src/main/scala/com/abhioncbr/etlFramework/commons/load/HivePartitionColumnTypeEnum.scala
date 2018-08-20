package com.abhioncbr.etlFramework.commons.load

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.{Context, ContextConstantEnum}
import org.joda.time.DateTime

object HivePartitionColumnTypeEnum extends Enumeration{
  type valueType = Value
  val DATE, YEAR, MONTH, DAY, HOUR, OTHER = Value

  def getValueType(valueTypeString: String): HivePartitionColumnTypeEnum.valueType = {
    val valueType = valueTypeString match {
      case "DATE" => HivePartitionColumnTypeEnum.DATE
      case "YEAR" => HivePartitionColumnTypeEnum.YEAR
      case "MONTH" => HivePartitionColumnTypeEnum.MONTH
      case "DAY" => HivePartitionColumnTypeEnum.DAY
      case "HOUR"  => HivePartitionColumnTypeEnum.HOUR
      case "OTHER" =>   HivePartitionColumnTypeEnum.OTHER
    }
    valueType
  }


  def getDataValue(valueType: HivePartitionColumnTypeEnum.valueType, otherValue: String = ""):  Any= {
    val output = valueType match {
      case DATE => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("yyyy-MM-dd")
      case YEAR => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("yyyy")
      case MONTH => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("MM")
      case DAY => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("dd")
      case HOUR => new DecimalFormat("00").format(Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).getHourOfDay)
      case OTHER => otherValue
    }
    output
  }

  def getPartitioningString(data: PartitioningData): String ={
    data.partitionColumns.map(col => s"${col.columnName} = '${getDataValue(col.columnValue)}'").mkString(" , ")
  }
}
