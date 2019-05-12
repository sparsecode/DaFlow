/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.abhioncbr.daflow.jobConf.xml

import com.abhioncbr.daflow.commons.common.GeneralParamConf


class ParseGeneralParamsSpec extends XmlJobConfBase{
  "ParseGeneralParam-fromXML" should "return GeneralParam object when xml node content is passed as an argument" in {
    val xmlContent: String = """<param order="1" name="{col1}" value="FIRST_DATE" defaultValue="SECOND_DATE"/>"""
    val generalParamObject: GeneralParamConf = ParseGeneralParam.fromXML(node(xmlContent))
    generalParamObject should not equal None

    generalParamObject.order should equal (1)
    generalParamObject.paramName should equal ("{col1}")

    generalParamObject.paramValue should equal ("FIRST_DATE")
    generalParamObject.paramDefaultValue should equal ("SECOND_DATE")
  }

  "ParseGeneralParam-fromXML" should "return GeneralParam object with no variable values when " +
    "xml node content is passed as an argument" in {
    val xmlContent: String = "<param/>"
    val generalParamObject: GeneralParamConf = ParseGeneralParam.fromXML(node(xmlContent))
    generalParamObject should not equal None

    generalParamObject.order should equal (-1)
    generalParamObject.paramName should equal ("")

    generalParamObject.paramValue should equal ("")
    generalParamObject.paramDefaultValue should equal ("")

    val xmlOtherContent: String = "<abc/>"
    val generalParamsOtherObject: GeneralParamConf = ParseGeneralParam.fromXML(node(xmlOtherContent))
    generalParamsOtherObject should not equal None

    generalParamsOtherObject.order should equal (-1)
    generalParamsOtherObject.paramName should equal ("")

    generalParamsOtherObject.paramValue should equal ("")
    generalParamsOtherObject.paramDefaultValue should equal ("")
  }

  "ParseGeneralParams-fromXML" should "return array of GeneralParam objects of length 2" in {
    val xmlContent: String = """<formatArgValues>
                            |<param order="1" name="{col1}" value="{val1}" defaultValue="{val1}"/>
                            |<param order="2" name="{col2}" value="{val2}" defaultValue="{val2}"/>
                            |</formatArgValues>""".stripMargin
    val generalParamObjectArray: Array[GeneralParamConf] = ParseGeneralParams.fromXML(node(xmlContent))
    generalParamObjectArray should not equal None
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
    val xmlContent: String = """<formatArgValues></formatArgValues>""".stripMargin

    val generalParamObjectArray: Array[GeneralParamConf] = ParseGeneralParams.fromXML(node(xmlContent))
    generalParamObjectArray should not equal None
    generalParamObjectArray.length should equal (0)
  }
}
