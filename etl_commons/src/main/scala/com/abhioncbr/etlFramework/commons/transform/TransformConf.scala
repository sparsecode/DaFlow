package com.abhioncbr.etlFramework.commons.transform

case class TransformConf(transformSteps: List[TransformStepConf], validateTransformedData: Boolean)
case class TransformStepConf(order: Int, rules: Map[String, TransformRuleConf])
case class TransformRuleConf(ruleType: String, condition: String, ruleAttributesMap: Map[String,String])