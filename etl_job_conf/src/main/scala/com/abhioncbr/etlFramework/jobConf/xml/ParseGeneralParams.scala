package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam

object ParseGeneralParams {
  def fromXML(node: scala.xml.NodeSeq, nodeTag: String): Array[GeneralParam] = {
    Array[GeneralParam]((node \ nodeTag \ "param").toList map { s => ParseGeneralParam.fromXML(s) }: _*)
  }
}

object ParseGeneralParam {
  def fromXML(node: scala.xml.NodeSeq): GeneralParam = {
    val order = ParseUtil.parseInt((node  \ "@order").text)
    val paramName = (node \ "@name").text
    val paramValue = (node \ "@value").text
    val paramDefaultValue = (node \ "@defaultValue").text
    GeneralParam(order, paramName, paramValue, paramDefaultValue)
  }
}
