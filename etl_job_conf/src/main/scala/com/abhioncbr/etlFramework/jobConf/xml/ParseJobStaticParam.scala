package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum
import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.job.JobStaticParam

object ParseJobStaticParam {
  def fromXML(node: scala.xml.NodeSeq): JobStaticParam = {
    JobStaticParam(processFrequency = ProcessFrequencyEnum.getProcessFrequencyEnum((node \ "@frequency").text),
      jobName = (node \ "@jobName").text,
      publishStats = ParseUtil.parseBoolean((node \ "@publishStats").text),
      otherParams = ParseUtil.parseNode[Array[GeneralParam]](node \ "otherParams",None, ParseGeneralParams.fromXML) //Some(ParseGeneralParams.fromXML(node, nodeTag= "otherParams"))
    )
  }
}
