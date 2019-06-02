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

import com.abhioncbr.daflow.commons.conf.transform.TransformRuleConf
import com.abhioncbr.daflow.commons.conf.transform.TransformStepConf

class ParseTransformRuleSpec extends XmlJobConfBase {
  val groupLit: String = "group"
  val scopeLit: String = "scope"

  "ParseTransformRule-SCHEMA_TRANSFORMATION" should "return all variables initialized TransformRuleConf Object" in {
    val xmlContent =
      """<rule type="SCHEMA_TRANSFORMATION" group="1" failedFieldLimit="10" failedRowLimit="10">
        |<condition>test_string2</condition></rule>""".stripMargin

    val transformRuleConfObject1: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject1 should not equal None
    transformRuleConfObject1.condition should be ("test_string2")
    transformRuleConfObject1.ruleAttributesMap.size should be (4)
    transformRuleConfObject1.ruleAttributesMap(groupLit) should be ("1")
    transformRuleConfObject1.ruleType should be ("SCHEMA_TRANSFORMATION")
    transformRuleConfObject1.ruleAttributesMap("failedFieldLimit") should be ("10")
    transformRuleConfObject1.ruleAttributesMap("failedRowLimit") should be ("10")
  }

  "ParseTransformRule-SELECT" should "return all variables initialized TransformRuleConf object " in {
    val xmlContent = """<rule type="SELECT" group='1'><condition>{col1}, {col2}, {col3}</condition></rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal None
    transformRuleConfObject.ruleType should be ("SELECT")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap(groupLit) should be ("1")
    transformRuleConfObject.condition should be ("{col1}, {col2}, {col3}")
  }

  "ParseTransformRule-PARTITION" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = """<rule type='PARTITION' group='1' scope="MERGE"><condition>{col1} is not null</condition></rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal None
    transformRuleConfObject.ruleType should be ("PARTITION")
    transformRuleConfObject.ruleAttributesMap.size should be (3)
    transformRuleConfObject.ruleAttributesMap(groupLit) should be ("1")
    transformRuleConfObject.condition should be ("{col1} is not null")
    transformRuleConfObject.ruleAttributesMap(scopeLit) should be ("MERGE")
  }

  "ParseTransformRule-EXPLODE" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = """<rule type='EXPLODE' group='11'><condition>{cond2}</condition></rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal None
    transformRuleConfObject.condition should be ("{cond2}")
    transformRuleConfObject.ruleType should be ("EXPLODE")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap(groupLit) should be ("11")
  }

  "ParseTransformRule-NIL" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = """<rule type='NIL' group='12'/>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal None
    transformRuleConfObject.condition should be ("")
    transformRuleConfObject.ruleType should be ("NIL")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap(groupLit) should be ("12")
  }

  "ParseTransformRule-MERGE" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = """<rule type='MERGE' group='1' mergeGroup='11,12'/>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal None
    transformRuleConfObject.condition should be ("")
    transformRuleConfObject.ruleType should be ("MERGE")
    transformRuleConfObject.ruleAttributesMap.size should be (3)
    transformRuleConfObject.ruleAttributesMap(groupLit) should be ("1")
    transformRuleConfObject.ruleAttributesMap("mergeGroup") should be ("11,12")
  }

  "ParseTransformRule-FILTER" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = """<rule type="FILTER" group='1'>
                            <condition>{col1} like 'my%'</condition>
                          </rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal None
    transformRuleConfObject.ruleType should be ("FILTER")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap(groupLit) should be ("1")
    transformRuleConfObject.condition should be ("{col1} like 'my%'")
  }

  "ParseTransformStep" should "return TransformStepConf object with array of TransformRuleConf object" in {
    val xmlContent = """<step order="1">
                            <rule type="FILTER" group='feed1'><condition>{col1} like 'my%'</condition></rule>
                            <rule type='MERGE' group='feed2' mergeGroup='11,12'/>
                          </step>"""
    val transformStepConfObject: TransformStepConf = ParseTransformStep.fromXML(node(xmlContent))
    transformStepConfObject should not equal None
    transformStepConfObject.order should be (1)
    transformStepConfObject.rules.size should be (2)
    transformStepConfObject.rules.head._1 should be ("feed1")
    transformStepConfObject.rules.last._1 should be ("feed2")
    transformStepConfObject.rules.last._2.ruleType should be ("MERGE")
    transformStepConfObject.rules.head._2.ruleType should be ("FILTER")
  }

  "ParseTransformStep" should "return TransformStepConf object with array of TransformRuleConf object of zero length" in {
    val xmlContent = """<step order="1"></step>"""
    val transformStepConfObject: TransformStepConf = ParseTransformStep.fromXML(node(xmlContent))
    transformStepConfObject should not equal None
    transformStepConfObject.order should be (1)
    transformStepConfObject.rules.size should be (0)
  }
}
