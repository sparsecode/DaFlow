package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.FieldMapping

object ParseFieldMappings {
  def fromXML(node: scala.xml.NodeSeq): List[FieldMapping] = {
    List[FieldMapping]((node \ "fieldMapping").toList map { s => ParseFieldMapping.fromXML(s) }: _*)
  }
}

object ParseFieldMapping {
  def fromXML(node: scala.xml.NodeSeq): FieldMapping = {
    FieldMapping(sourceFieldName = (node \ "@sourceName").text,
      targetFieldName = (node \ "@targetName").text)
  }
}
