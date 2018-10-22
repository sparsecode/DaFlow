package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum
import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.job.JobStaticParam

object ParseJobStaticParam {
  def fromXML(node: scala.xml.NodeSeq): JobStaticParam = {
    JobStaticParam(processFrequency = ProcessFrequencyEnum.getProcessFrequencyEnum((node \ "@frequency").text),
      feedName = (node \ "@jobName").text,
      publishStats = ParseUtil.parseBoolean((node \ "@publishStats").text),
      otherParams = Some(Array[GeneralParam]((node \ "otherParams" \ "param").toList map { s => ParseGeneralParams.fromXML(s) }: _*))
    )
  }
}
