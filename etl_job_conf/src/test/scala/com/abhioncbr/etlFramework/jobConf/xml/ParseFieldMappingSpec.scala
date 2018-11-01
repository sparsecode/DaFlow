package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.FieldMappingConf

class ParseFieldMappingSpec extends XmlJobConfBase {

  "ParseFieldMapping-fromXML" should "return FieldMapping object" in {
    val xmlContent: String = s"""<fieldMapping sourceName="source" targetName="target"/>"""
    val filedMappingObject: FieldMappingConf  = ParseFieldMapping.fromXML(node(xmlContent))
    filedMappingObject should not equal null
    filedMappingObject.sourceFieldName should be ("source")
    filedMappingObject.targetFieldName should be ("target")
  }


  "ParseFieldMappings-fromXML" should "return FieldMapping object" in {
    val xmlContent: String =
      s"""<node>
         |<fieldMapping sourceName="source1" targetName="target1"/>
         |<fieldMapping sourceName="source2" targetName="target2"/>
         |<fieldMapping sourceName="source3" targetName="target3"/>
         |</node>
       """.stripMargin
    val filedMappingArrayObject: List[FieldMappingConf]  = ParseFieldMappings.fromXML(node(xmlContent))
    filedMappingArrayObject should not equal null
    filedMappingArrayObject.length should be (3)
    filedMappingArrayObject.head.sourceFieldName should be ("source1")
    filedMappingArrayObject.head.targetFieldName should be ("target1")
  }
}
