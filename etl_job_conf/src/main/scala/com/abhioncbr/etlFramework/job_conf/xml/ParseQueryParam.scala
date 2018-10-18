package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.query.{QueryParam, QueryParamTypeEnum}

object ParseQueryParam {
  def fromXML(node: scala.xml.NodeSeq): QueryParam = {
    val order = ParseUtil.parseInt((node  \ "@order").text)
    val paramName = (node \ "@name").text
    val paramValue = (node \ "@value").text
    val paramDefaultValue = (node \ "@default_value").text
    QueryParam(order, paramName, QueryParamTypeEnum.getValueType(paramValue), paramDefaultValue)
  }
}
