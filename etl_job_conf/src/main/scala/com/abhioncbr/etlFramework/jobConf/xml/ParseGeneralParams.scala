package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParamConf

object ParseGeneralParams {
  def fromXML(node: scala.xml.NodeSeq): Array[GeneralParamConf] = {
    Array[GeneralParamConf]((node \ "param").toList map { s => ParseGeneralParam.fromXML(s) }: _*)
  }
}

object ParseGeneralParam {
  def fromXML(node: scala.xml.NodeSeq): GeneralParamConf = {
    val order = ParseUtil.parseInt((node  \ "@order").text)
    val paramName = (node \ "@name").text
    val paramValue = (node \ "@value").text
    val paramDefaultValue = (node \ "@defaultValue").text
    GeneralParamConf(order, paramName, paramValue, paramDefaultValue)
  }
}
