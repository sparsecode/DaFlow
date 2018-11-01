package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParamConf
import com.abhioncbr.etlFramework.commons.load.PartitioningDataConf

object ParsePartitioningData {
  def fromXML(node: scala.xml.NodeSeq): PartitioningDataConf = {
    val coalesce = ParseUtil.parseBoolean((node \ "@coalescePartition").text)
    val overwrite = ParseUtil.parseBoolean((node \ "@overwritePartition").text)
    val coalesceCount = ParseUtil.parseInt((node \ "@coalescePartitionCount").text)
    val partitionColumns = List[GeneralParamConf]((node \ "partitionColumns" \ "column").toList map { s => ParseGeneralParam.fromXML(s) }: _*)
    PartitioningDataConf(coalesce = coalesce, overwrite = overwrite, coalesceCount = coalesceCount, partitionColumns = partitionColumns)
  }
}
