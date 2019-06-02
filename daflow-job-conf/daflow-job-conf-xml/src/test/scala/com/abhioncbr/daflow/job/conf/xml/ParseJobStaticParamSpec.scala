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

package com.abhioncbr.daflow.job.conf.xml

import com.abhioncbr.daflow.commons.conf.JobStaticParamConf

class ParseJobStaticParamSpec extends XmlJobConfBase {

  "ParseJobStaticParam-fromXML" should "return JobStaticParam object" in {
    val xmlContent: String = """<jobStaticParam jobName="Job1" frequency="ONCE" publishStats="false"/>"""
    val jobStaticParamObject: JobStaticParamConf = ParseJobStaticParam.fromXML(node(xmlContent))
    jobStaticParamObject should not equal None
    jobStaticParamObject.jobName should be ("Job1")
    jobStaticParamObject.processFrequency.toString should be ("ONCE")
    jobStaticParamObject.publishStats should be (false)
  }

  "ParseJobStaticParam-fromXML" should "return JobStaticParam object with otherParams also" in {
    val xmlContent: String =
      """<jobStaticParam jobName="Job1" frequency="ONCE" publishStats="false">
        |<otherParams>
        |<param order="1" name="{col1}" value="{val1}" defaultValue="{val1}"/>
        |<param order="2" name="{col2}" value="{val2}" defaultValue="{val2}"/>
        |</otherParams></jobStaticParam>""".stripMargin
    val jobStaticParamObject: JobStaticParamConf = ParseJobStaticParam.fromXML(node(xmlContent))
    jobStaticParamObject should not equal None
    jobStaticParamObject.jobName should be ("Job1")
    jobStaticParamObject.processFrequency.toString should be ("ONCE")
    jobStaticParamObject.publishStats should be (false)
    jobStaticParamObject.otherParams should not equal None
    jobStaticParamObject.otherParams.get should not equal None
    jobStaticParamObject.otherParams.get.length should be (2)
  }
}
