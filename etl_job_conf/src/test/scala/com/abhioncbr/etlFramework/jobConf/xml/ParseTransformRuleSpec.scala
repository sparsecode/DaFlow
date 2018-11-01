package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.transform.{TransformRuleConf, TransformStepConf}

class ParseTransformRuleSpec extends XmlJobConfBase {

  "ParseTransformRule-SCHEMA_TRANSFORMATION" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = s"""<rule type="SCHEMA_TRANSFORMATION" group="1" failedFieldLimit="10" failedRowLimit="10">
                            <condition>test_string2</condition>
                      </rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal null
    transformRuleConfObject.condition should be ("test_string2")
    transformRuleConfObject.ruleAttributesMap.size should be (4)
    transformRuleConfObject.ruleAttributesMap("group") should be ("1")
    transformRuleConfObject.ruleType should be ("SCHEMA_TRANSFORMATION")
    transformRuleConfObject.ruleAttributesMap("failedFieldLimit") should be ("10")
    transformRuleConfObject.ruleAttributesMap("failedRowLimit") should be ("10")
  }

  "ParseTransformRule-SELECT" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = s"""<rule type="SELECT" group='1'>
                            <condition>{col1}, {col2}, {col3}</condition>
                         </rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal null
    transformRuleConfObject.ruleType should be ("SELECT")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap("group") should be ("1")
    transformRuleConfObject.condition should be ("{col1}, {col2}, {col3}")
  }

  "ParseTransformRule-PARTITION" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = s"""<rule type='PARTITION' group='1' scope="MERGE"><condition>{col1} is not null</condition></rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal null
    transformRuleConfObject.ruleType should be ("PARTITION")
    transformRuleConfObject.ruleAttributesMap.size should be (3)
    transformRuleConfObject.ruleAttributesMap("group") should be ("1")
    transformRuleConfObject.condition should be ("{col1} is not null")
    transformRuleConfObject.ruleAttributesMap("scope") should be ("MERGE")
  }

  "ParseTransformRule-EXPLODE" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = s"""<rule type='EXPLODE' group='11'><condition>{cond2}</condition></rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal null
    transformRuleConfObject.condition should be ("{cond2}")
    transformRuleConfObject.ruleType should be ("EXPLODE")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap("group") should be ("11")
  }

  "ParseTransformRule-NIL" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = s"""<rule type='NIL' group='12'/>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal null
    transformRuleConfObject.condition should be ("")
    transformRuleConfObject.ruleType should be ("NIL")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap("group") should be ("12")
  }

  "ParseTransformRule-MERGE" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = s"""<rule type='MERGE' group='1' mergeGroup='11,12'/>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal null
    transformRuleConfObject.condition should be ("")
    transformRuleConfObject.ruleType should be ("MERGE")
    transformRuleConfObject.ruleAttributesMap.size should be (3)
    transformRuleConfObject.ruleAttributesMap("group") should be ("1")
    transformRuleConfObject.ruleAttributesMap("mergeGroup") should be ("11,12")
  }

  "ParseTransformRule-FILTER" should "return TransformRuleConf object with all variables initialized" in {
    val xmlContent = s"""<rule type="FILTER" group='1'>
                            <condition>{col1} like 'my%'</condition>
                          </rule>"""
    val transformRuleConfObject: TransformRuleConf = ParseTransformRule.fromXML(node(xmlContent))
    transformRuleConfObject should not equal null
    transformRuleConfObject.ruleType should be ("FILTER")
    transformRuleConfObject.ruleAttributesMap.size should be (2)
    transformRuleConfObject.ruleAttributesMap("group") should be ("1")
    transformRuleConfObject.condition should be ("{col1} like 'my%'")
  }

  "ParseTransformStep" should "return TransformStepConf object with array of TransformRuleConf object" in {
    val xmlContent = s"""<step order="1">
                            <rule type="FILTER" group='feed1'><condition>{col1} like 'my%'</condition></rule>
                            <rule type='MERGE' group='feed2' mergeGroup='11,12'/>
                          </step>"""
    val TransformStepConfObject: TransformStepConf = ParseTransformStep.fromXML(node(xmlContent))
    TransformStepConfObject should not equal null
    TransformStepConfObject.order should be (1)
    TransformStepConfObject.rules.size should be (2)
    TransformStepConfObject.rules.head._1 should be ("feed1")
    TransformStepConfObject.rules.last._1 should be ("feed2")
    TransformStepConfObject.rules.last._2.ruleType should be ("MERGE")
    TransformStepConfObject.rules.head._2.ruleType should be ("FILTER")
  }

  "ParseTransformStep" should "return TransformStepConf object with array of TransformRuleConf object of zero length" in {
    val xmlContent = s"""<step order="1">
                          </step>"""
    val TransformStepConfObject: TransformStepConf = ParseTransformStep.fromXML(node(xmlContent))
    TransformStepConfObject should not equal null
    TransformStepConfObject.order should be (1)
    TransformStepConfObject.rules.size should be (0)
  }
}
