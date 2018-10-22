package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.FieldMapping

object ParseFieldMapping {
  def fromXML(node: scala.xml.NodeSeq): FieldMapping = {
    FieldMapping(sourceFieldName = (node \ "@sourceName").text,
      targetFieldName = (node \ "@targetName").text)
  }
}
