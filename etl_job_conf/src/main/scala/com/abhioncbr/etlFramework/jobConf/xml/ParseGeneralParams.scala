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
