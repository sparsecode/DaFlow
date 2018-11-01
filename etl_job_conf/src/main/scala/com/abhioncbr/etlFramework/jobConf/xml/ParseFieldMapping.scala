package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.FieldMappingConf

object ParseFieldMappings {
  def fromXML(node: scala.xml.NodeSeq): List[FieldMappingConf] = {
    List[FieldMappingConf]((node \ "fieldMapping").toList map { s => ParseFieldMapping.fromXML(s) }: _*)
  }
}

object ParseFieldMapping {
  def fromXML(node: scala.xml.NodeSeq): FieldMappingConf = {
    FieldMappingConf(sourceFieldName = (node \ "@sourceName").text,
      targetFieldName = (node \ "@targetName").text)
  }
}
