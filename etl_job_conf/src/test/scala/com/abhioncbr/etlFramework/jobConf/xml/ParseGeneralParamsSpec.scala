package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParamConf


class ParseGeneralParamsSpec extends XmlJobConfBase{
  "ParseGeneralParam-fromXML" should "return GeneralParam object when xml node content is passed as an argument" in {
    val xmlContent: String = s"""<param order="1" name="{col1}" value="FIRST_DATE" defaultValue="SECOND_DATE"/>"""
    val generalParamObject: GeneralParamConf = ParseGeneralParam.fromXML(node(xmlContent))
    generalParamObject should not equal null

    generalParamObject.order should equal (1)
    generalParamObject.paramName should equal ("{col1}")

    generalParamObject.paramValue should equal ("FIRST_DATE")
    generalParamObject.paramDefaultValue should equal ("SECOND_DATE")
  }

  "ParseGeneralParam-fromXML" should "return GeneralParam object with  no variable values when xml node content is passed as an argument" in {
    val xmlContent: String = s"""<param/>"""
    val generalParamObject: GeneralParamConf = ParseGeneralParam.fromXML(node(xmlContent))
    generalParamObject should not equal null

    generalParamObject.order should equal (-1)
    generalParamObject.paramName should equal ("")

    generalParamObject.paramValue should equal ("")
    generalParamObject.paramDefaultValue should equal ("")

    val xmlOtherContent: String = s"""<abc/>"""
    val generalParamsOtherObject: GeneralParamConf = ParseGeneralParam.fromXML(node(xmlOtherContent))
    generalParamsOtherObject should not equal null

    generalParamsOtherObject.order should equal (-1)
    generalParamsOtherObject.paramName should equal ("")

    generalParamsOtherObject.paramValue should equal ("")
    generalParamsOtherObject.paramDefaultValue should equal ("")
  }

  "ParseGeneralParams-fromXML" should "return array of GeneralParam objects of length 2" in {
    val xmlContent: String =s"""<formatArgValues>
         |<param order="1" name="{col1}" value="{val1}" defaultValue="{val1}"/>
         |<param order="2" name="{col2}" value="{val2}" defaultValue="{val2}"/>
         |</formatArgValues>""".stripMargin

    val generalParamObjectArray: Array[GeneralParamConf] = ParseGeneralParams.fromXML(node(xmlContent))
    generalParamObjectArray should not equal null
    generalParamObjectArray.length should equal (2)

    generalParamObjectArray(0).order should equal (1)
    generalParamObjectArray(0).paramName should equal ("{col1}")
    generalParamObjectArray(0).paramValue should equal ("{val1}")
    generalParamObjectArray(0).paramDefaultValue should equal ("{val1}")

    generalParamObjectArray(1).order should equal (2)
    generalParamObjectArray(1).paramName should equal ("{col2}")
    generalParamObjectArray(1).paramValue should equal ("{val2}")
    generalParamObjectArray(1).paramDefaultValue should equal ("{val2}")
  }

  "ParseGeneralParams-fromXML" should "return array of GeneralParam objects of length 0" in {
    val xmlContent: String =s"""<formatArgValues>
                               |</formatArgValues>""".stripMargin

    val generalParamObjectArray: Array[GeneralParamConf] = ParseGeneralParams.fromXML(node(xmlContent))
    generalParamObjectArray should not equal null
    generalParamObjectArray.length should equal (0)
  }
}
