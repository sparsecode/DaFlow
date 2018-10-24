package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.load.{PartitionColumn, PartitionColumnTypeEnum, PartitioningData}

object ParsePartitioningData {
  def fromXML(node: scala.xml.NodeSeq): PartitioningData = {
    val coalesce = ParseUtil.parseBoolean((node \ "coalescePartition").text)
    val overwrite = ParseUtil.parseBoolean((node \ "overwritePartition").text)
    val coalesceCount = ParseUtil.parseInt((node \ "coalescePartitionCount").text)
    val partitionColumns = List[PartitionColumn]((node \ "partitionColumns" \ "column").toList map { s => ParsePartitionColumn.fromXML(s) }: _*)
    val dataPath: FilePath = ParseDataPath.fromXML(node \ "dataPath").get
    PartitioningData(coalesce = coalesce, overwrite = overwrite, coalesceCount = coalesceCount, partitionColumns = partitionColumns, dataPath = dataPath)
  }
}

object ParsePartitionColumn {
  def fromXML(node: scala.xml.NodeSeq): PartitionColumn = {
    val name = (node  \ "@name").text
    val value = (node \ "@value").text
    PartitionColumn(name, PartitionColumnTypeEnum.getValueType(value))
  }
}
