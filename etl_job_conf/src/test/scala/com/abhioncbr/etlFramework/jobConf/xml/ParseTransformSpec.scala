package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.transform.TransformConf

class ParseTransformSpec extends XmlJobConfBase {

  "ParseTransform" should "return TransformConf object with array of TransformStepsConf" in {
    val xmlContent = s"""<transform><step order="23">
                            <rule type="FILTER" group='feed1'><condition>{col1} like 'my%'</condition></rule>
                            <rule type='MERGE' group='feed2' mergeGroup='11,12'/>
                          </step></transform>"""
    val TransformConfObject: TransformConf = ParseTransform.fromXML(node(xmlContent))
    TransformConfObject should not equal null
    TransformConfObject.validateTransformedData should be (false)
    TransformConfObject.transformSteps.size should be (1)
    TransformConfObject.transformSteps.head.order should be (23)
  }
}
