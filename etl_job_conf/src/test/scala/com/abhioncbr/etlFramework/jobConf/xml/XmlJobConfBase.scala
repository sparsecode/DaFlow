package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.CommonSpec

import scala.xml.XML

class XmlJobConfBase extends CommonSpec{

  val node: String => scala.xml.NodeSeq  = (xmlContent: String) => { XML.loadString(xmlContent) }

}
