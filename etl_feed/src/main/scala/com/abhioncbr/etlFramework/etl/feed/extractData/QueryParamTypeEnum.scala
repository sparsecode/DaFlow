package com.abhioncbr.etlFramework.etl.feed.extractData

import com.abhioncbr.etlFramework.etl.feed.common.{Context, ContextConstantEnum, QueryParam}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by Abhishek on 21/2/17.
  */
object QueryParamTypeEnum extends Enumeration {
  type valueType = Value
  val VENTURE, FIRST_DATE_DAY, FIRST_DATE_MONTH, FIRST_DATE_YEAR, FIRST_DATE, FIRST_DATE_WITH_ZONE,
    BOB_LIVE, ASC_LIVE, ASC_ITEM= Value

  def getValueType(valueTypeString: String): QueryParamTypeEnum.valueType = {
    val valueType = valueTypeString match {
      case "VENTURE" =>  QueryParamTypeEnum.VENTURE
      case "FIRST_DATE" =>  QueryParamTypeEnum.FIRST_DATE
      case "FIRST_DATE_DAY" => QueryParamTypeEnum.FIRST_DATE_DAY
      case "FIRST_DATE_YEAR" =>  QueryParamTypeEnum.FIRST_DATE_YEAR
      case "FIRST_DATE_MONTH" =>  QueryParamTypeEnum.FIRST_DATE_MONTH
      case "FIRST_DATE_WITH_ZONE" =>  QueryParamTypeEnum.FIRST_DATE_WITH_ZONE
      //TODO: we can extend it further for any type.

      //extended for DB names along with venture (for hive queries)
      case "BOB_LIVE" => QueryParamTypeEnum.BOB_LIVE
      case "ASC_LIVE" => QueryParamTypeEnum.ASC_LIVE
      case "ASC_ITEM" => QueryParamTypeEnum.ASC_ITEM
    }
    valueType
  }

  def getDataValue(valueType: QueryParamTypeEnum.valueType, otherValue: String = ""):  Object= {
    val output = valueType match {
      case VENTURE => Context.getContextualObject[DateTime](ContextConstantEnum.VENTURE)
      case FIRST_DATE => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("YYYY-MM-dd")
      case FIRST_DATE_DAY => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("dd")
      case FIRST_DATE_YEAR => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("YYYY")
      case FIRST_DATE_MONTH => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).toString("MM")
      case FIRST_DATE_WITH_ZONE => Context.getContextualObject[DateTime](ContextConstantEnum.FIRST_DATE).withZone(DateTimeZone.UTC).toString("YYYY-MM-dd")
      //TODO: we can extend it further for any type.

      //extended for DB names along with venture (for hive queries)
      case BOB_LIVE => s"${Context.getContextualObject[DateTime](ContextConstantEnum.VENTURE)}_bob_live"
      case ASC_LIVE => s"${Context.getContextualObject[DateTime](ContextConstantEnum.VENTURE)}_asc_live"
      case ASC_ITEM => s"${Context.getContextualObject[DateTime](ContextConstantEnum.VENTURE)}_asc_item"
    }
    output
  }

  def getParamsValue(paramList: List[QueryParam]): Array[Object] ={
    paramList.map(queryParam => (queryParam.order, getDataValue(queryParam.paramValue))).sortBy(_._1).map(_._2).toArray
  }
}

object Test extends App{
  println("aa")
  val test: List[QueryParam] = List(new QueryParam(1, "FIRST_DATE_YEAR", QueryParamTypeEnum.getValueType("FIRST_DATE_YEAR"), ""))
  println(QueryParamTypeEnum.getValueType("FIRST_DATE_YEAR"))
}

