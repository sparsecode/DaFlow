package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.CommonSpec
import com.abhioncbr.etlFramework.commons.common.file.FileNameParam

import scala.xml.XML

class ParseDataPathSpec extends XmlJobConfBase{

  "ParseFileName" should "return FileNameParam object with name params" in {
    val xmlContent: String = "<fileName> <prefix>json_data</prefix> <suffix>json</suffix> </fileName>"
    val FileNameParamObject: FileNameParam = ParseFileName.fromXML(node(xmlContent))
    FileNameParamObject should not equal null

    FileNameParamObject.fileNamePrefix should equal (Some("json_data"))
    FileNameParamObject.fileNamePrefix.get should equal ("json_data")

    FileNameParamObject.fileNameSuffix should equal (Some("json"))
    FileNameParamObject.fileNameSuffix.get should equal ("json")

    FileNameParamObject.fileNameSeparator should equal (Some("."))
  }

  "ParseFileName" should "return FileNameParam object with refix & suffix as None" in {
    val xmlContent: String = "<fileName> </fileName>"
    val FileNameParamObject: FileNameParam = ParseFileName.fromXML(node(xmlContent))
    FileNameParamObject should not equal null

    FileNameParamObject.fileNamePrefix should equal (None)
    FileNameParamObject.fileNameSuffix should equal (None)
    FileNameParamObject.fileNameSeparator should equal (Some("."))

    val xmlOtherContent: String = "<abc> </abc>"
    val FileNameParamOtherObject: FileNameParam = ParseFileName.fromXML(node(xmlContent))
    FileNameParamOtherObject should not equal null

    FileNameParamOtherObject.fileNamePrefix should equal (None)
    FileNameParamOtherObject.fileNameSuffix should equal (None)
    FileNameParamOtherObject.fileNameSeparator should equal (Some("."))
  }

}
