package com.abhioncbr.etlFramework.etl.feed.loadData.hive

import java.text.DecimalFormat

import com.abhioncbr.etlFramework.etl.feed.common.{Context, ContextConstantEnum, PartitioningData}
import org.joda.time.DateTime

object PartitionColumnTypeEnum extends Enumeration{
  type valueType = Value
  val VENTURE, DATE, YEAR, MONTH, DAY, HOUR, OTHER = Value

  def getValueType(valueTypeString: String): PartitionColumnTypeEnum.valueType = {
    val valueType = valueTypeString match {
      case "VENTURE" => PartitionColumnTypeEnum.VENTURE
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
      case VENTURE => Context.getContextualObject[String](ContextConstantEnum.VENTURE)
      case DATE => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("yyyy-MM-dd")
      case YEAR => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("yyyy")
      case MONTH => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("MM")
      case DAY => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("dd")
      case HOUR => new DecimalFormat("00").format(Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).getHourOfDay)
      case OTHER => otherValue
      //TODO: we can extend it further for weekly, monthly, date_range.
    }
    output
  }

  def getPartitioningString(data: PartitioningData): String ={
    data.partitionColumns.map(col => s"${col.columnName} = '${getDataValue(col.columnValue)}'").mkString(" , ")
  }
}
