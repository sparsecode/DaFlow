package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.load.PartitioningData

object ParsePartitioningData {
  def fromXML(node: scala.xml.NodeSeq): PartitioningData = {
    val coalesce = ParseUtil.parseBoolean((node \ "@coalescePartition").text)
    val overwrite = ParseUtil.parseBoolean((node \ "@overwritePartition").text)
    val coalesceCount = ParseUtil.parseInt((node \ "@coalescePartitionCount").text)
    val partitionColumns = List[GeneralParam]((node \ "partitionColumns" \ "column").toList map { s => ParseGeneralParam.fromXML(s) }: _*)
    PartitioningData(coalesce = coalesce, overwrite = overwrite, coalesceCount = coalesceCount, partitionColumns = partitionColumns)
  }
}
