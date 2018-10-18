package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam

object ParseGeneralParams {
  def fromXML(node: scala.xml.NodeSeq): GeneralParam = {
    val order = ParseUtil.parseInt((node  \ "@order").text)
    val paramName = (node \ "@name").text
    val paramValue = (node \ "@value").text
    val paramDefaultValue = (node \ "@default_value").text
    GeneralParam(order, paramName, paramValue, paramDefaultValue)
  }
}
