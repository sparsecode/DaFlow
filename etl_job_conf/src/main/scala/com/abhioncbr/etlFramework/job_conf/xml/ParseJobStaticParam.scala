package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.ProcessFrequencyEnum
import com.abhioncbr.etlFramework.commons.job.JobStaticParam

object ParseJobStaticParam {
  def fromXML(node: scala.xml.NodeSeq): JobStaticParam = {
    val frequency  = (node \ "frequency").text
    JobStaticParam(processFrequency = ProcessFrequencyEnum.getProcessFrequencyEnum(frequency),
      feedName = (node \ "feed_name").text,
      publishStats = ParseUtil.parseBoolean((node \ "publish_stats").text))
  }
}
