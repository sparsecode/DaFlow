package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.FieldMapping

object ParseFieldMapping {
  def fromXML(node: scala.xml.NodeSeq): FieldMapping = {
    FieldMapping(sourceFieldName = (node \ "@source_name").text,
      targetFieldName = (node \ "@target_name").text)
  }
}
