package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.transform.{TransformConf, TransformStepConf}

object ParseTransform {
  def fromXML(node: scala.xml.NodeSeq): TransformConf = {
    val steps: List[TransformStepConf] = List[TransformStepConf]((node \ "step").toList map { s => ParseTransformStep.fromXML(s) }: _*)
    TransformConf(transformSteps = steps, validateTransformedData = ParseUtil.parseBoolean((node \ "@validateTransformedData").text))
  }
}
