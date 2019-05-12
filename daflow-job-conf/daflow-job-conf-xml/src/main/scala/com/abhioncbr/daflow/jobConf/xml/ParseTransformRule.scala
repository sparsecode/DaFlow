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

import com.abhioncbr.daflow.commons.transform
import com.abhioncbr.daflow.commons.transform.{TransformRuleConf, TransformStepConf}
import com.abhioncbr.daflow.commons.transform.TransformRuleConf

object ParseTransformStep {
  def fromXML(node: scala.xml.NodeSeq): TransformStepConf = {
    val order = ParseUtil.parseInt((node \ "@order").text)

    val rules: Map[String, TransformRuleConf] = List[TransformRuleConf]((node \ "rule").toList map {
      s => ParseTransformRule.fromXML(s)
    }: _*).map(rule => (rule.ruleAttributesMap("group"), rule)).toMap

    transform.TransformStepConf(order = order, rules = rules)
  }
}

object ParseTransformRule {
  def fromXML(node: scala.xml.NodeSeq): TransformRuleConf = {
    val ruleType = (node \ "@type").text
    val condition = (node \ "condition").text
    val ruleAttributesMap: Map[String, String] = node.head.attributes.map(meta => (meta.key, meta.value.toString)).toMap
    TransformRuleConf(ruleType = ruleType, condition = condition, ruleAttributesMap = ruleAttributesMap)
  }
}
