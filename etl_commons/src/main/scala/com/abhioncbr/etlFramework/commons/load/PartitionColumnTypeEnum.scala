package com.abhioncbr.etlFramework.commons.load

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.commons.{Context, ContextConstantEnum}
import org.joda.time.DateTime

object PartitionColumnTypeEnum extends Enumeration{
  type valueType = Value
  val DATE, YEAR, MONTH, DAY, HOUR, OTHER = Value

  def getValueType(valueTypeString: String): PartitionColumnTypeEnum.valueType = {
    val valueType = valueTypeString match {
      case "DATE" => PartitionColumnTypeEnum.DATE
      case "YEAR" => PartitionColumnTypeEnum.YEAR
      case "MONTH" => PartitionColumnTypeEnum.MONTH
      case "DAY" => PartitionColumnTypeEnum.DAY
      case "HOUR"  => PartitionColumnTypeEnum.HOUR
      case "OTHER" =>   PartitionColumnTypeEnum.OTHER
    }
    valueType
  }


  def getDataValue(valueType: PartitionColumnTypeEnum.valueType, otherValue: String = ""):  Any= {
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
