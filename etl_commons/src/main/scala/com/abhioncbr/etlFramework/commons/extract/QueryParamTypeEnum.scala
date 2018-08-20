package com.abhioncbr.etlFramework.commons.extract

import com.abhioncbr.etlFramework.commons.{Context, ContextConstantEnum}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by Abhishek on 21/2/17.
  */
object QueryParamTypeEnum extends Enumeration {
  type valueType = Value
  val FIRST_DATE_DAY, FIRST_DATE_MONTH, FIRST_DATE_YEAR, FIRST_DATE, FIRST_DATE_WITH_ZONE= Value

  def getValueType(valueTypeString: String): QueryParamTypeEnum.valueType = {
    val valueType = valueTypeString match {
      case "FIRST_DATE" =>  QueryParamTypeEnum.FIRST_DATE
      case "FIRST_DATE_DAY" => QueryParamTypeEnum.FIRST_DATE_DAY
      case "FIRST_DATE_YEAR" =>  QueryParamTypeEnum.FIRST_DATE_YEAR
      case "FIRST_DATE_MONTH" =>  QueryParamTypeEnum.FIRST_DATE_MONTH
      case "FIRST_DATE_WITH_ZONE" =>  QueryParamTypeEnum.FIRST_DATE_WITH_ZONE
    }
    valueType
  }

  def getDataValue(valueType: QueryParamTypeEnum.valueType, otherValue: String = ""):  Object= {
    val output = valueType match {
      case FIRST_DATE => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("YYYY-MM-dd")
      case FIRST_DATE_DAY => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("dd")
      case FIRST_DATE_YEAR => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("YYYY")
      case FIRST_DATE_MONTH => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("MM")
      case FIRST_DATE_WITH_ZONE => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).withZone(DateTimeZone.UTC).toString("YYYY-MM-dd")
    }
    output
  }

  def getParamsValue(paramList: List[QueryParam]): Array[Object] ={
    paramList.map(queryParam => (queryParam.order, getDataValue(queryParam.paramValue))).sortBy(_._1).map(_._2).toArray
  }
}
