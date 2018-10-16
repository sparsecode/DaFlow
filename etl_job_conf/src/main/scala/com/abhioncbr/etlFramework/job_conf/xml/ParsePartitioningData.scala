package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.load.{PartitionColumn, PartitionColumnTypeEnum, PartitioningData}

object ParsePartitioningData {
  def fromXML(node: scala.xml.NodeSeq): PartitioningData = {
    val coalesce = ParseUtil.parseBoolean((node \ "coalesce_partition").text)
    val overwrite = ParseUtil.parseBoolean((node \ "overwrite_partition").text)
    val coalesceCount = ParseUtil.parseInt((node \ "coalesce_partition_count").text)
    val partitionColumns = List[PartitionColumn]((node \ "partition_columns" \ "column").toList map { s => ParsePartitionColumn.fromXML(s) }: _*)
    val partitionFileInitialPath = (node \ "partition_file_initial_path").text
    PartitioningData(coalesce = coalesce, overwrite = overwrite, coalesceCount = coalesceCount, partitionColumns = partitionColumns, partitionFileInitialPath = partitionFileInitialPath)
  }
}

object ParsePartitionColumn {
  def fromXML(node: scala.xml.NodeSeq): PartitionColumn = {
    val name = (node  \ "@name").text
    val value = (node \ "@value").text
    PartitionColumn(name, PartitionColumnTypeEnum.getValueType(value))
  }
}
