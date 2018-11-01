package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.transform.{TransformRuleConf, TransformStepConf}

object ParseTransformStep {
  def fromXML(node: scala.xml.NodeSeq): TransformStepConf = {
    val order = ParseUtil.parseInt((node \ "@order").text)
    val rules: Map[String, TransformRuleConf] = List[TransformRuleConf]((node \ "rule").toList map { s => ParseTransformRule.fromXML(s) }: _*).map(rule => (rule.ruleAttributesMap("group"), rule)).toMap
    TransformStepConf(order = order, rules= rules)
  }
}

object ParseTransformRule {
  def fromXML(node: scala.xml.NodeSeq): TransformRuleConf = {
    val ruleType  = (node \ "@type").text
    val condition = (node \ "condition").text
    val ruleAttributesMap: Map[String, String] = node.head.attributes.map(meta => (meta.key, meta.value.toString)).toMap
    TransformRuleConf(ruleType = ruleType, condition = condition, ruleAttributesMap = ruleAttributesMap)
  }
}
