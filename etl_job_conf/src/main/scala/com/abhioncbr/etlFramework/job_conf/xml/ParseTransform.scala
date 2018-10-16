package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.transform.{DummyRule, Transform, TransformationRule, TransformationStep}

import scala.collection.immutable.ListMap

object ParseTransform {
  def fromXML(node: scala.xml.NodeSeq): Transform = {
    val rules = List[DummyRule]((node \ "rule").toList map { s => ParseRule.fromXML(s) }: _*).flatten(dummy => ParseRule.getRules(dummy))
    Transform(transformationSteps = getTransformationStep(rules), validateTransformedData = (node \ "validate_transformed_data").text.toBoolean)
  }

  def getTransformationStep(rules: List[TransformationRule]) : List[TransformationStep] ={
    val orderedRule = rules.map(rule => (rule.getOrder,rule)).groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    val transformationStepsMap = orderedRule.keys.zip(orderedRule.values.map(list => list.map(rule => (rule.getGroup, rule)).toMap))
    val transformationSteps: List[TransformationStep] = ListMap(transformationStepsMap.toSeq.sortBy(_._1):_*).
      map(entry => new TransformationStep(entry._1,entry._2)).toList


    transformationSteps
  }
}
