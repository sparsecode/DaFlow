package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.job.JobStaticParam

class ParseJobStaticParamSpec extends XmlJobConfBase {

  "ParseJobStaticParam-fromXML" should "return JobStaticParam object" in {
    val xmlContent: String = s"""<jobStaticParam jobName="Job1" frequency="ONCE" publishStats="false"/>"""
    val jobStaticParamObject: JobStaticParam = ParseJobStaticParam.fromXML(node(xmlContent))
    jobStaticParamObject should not equal null
    jobStaticParamObject.jobName should be ("Job1")
    jobStaticParamObject.processFrequency.toString should be ("ONCE")
    jobStaticParamObject.publishStats should be (false)
  }

  "ParseJobStaticParam-fromXML" should "return JobStaticParam object with otherParams also" in {
    val xmlContent: String =
      s"""<jobStaticParam jobName="Job1" frequency="ONCE" publishStats="false">
         |<otherParams>
         |<param order="1" name="{col1}" value="{val1}" defaultValue="{val1}"/>
         |<param order="2" name="{col2}" value="{val2}" defaultValue="{val2}"/>
         |</otherParams></jobStaticParam>
       """.stripMargin
    val jobStaticParamObject: JobStaticParam = ParseJobStaticParam.fromXML(node(xmlContent))
    jobStaticParamObject should not equal null
    jobStaticParamObject.jobName should be ("Job1")
    jobStaticParamObject.processFrequency.toString should be ("ONCE")
    jobStaticParamObject.publishStats should be (false)
    jobStaticParamObject.otherParams should not equal null
    jobStaticParamObject.otherParams.get should not equal null
    jobStaticParamObject.otherParams.get.length should be (2)
  }
}
