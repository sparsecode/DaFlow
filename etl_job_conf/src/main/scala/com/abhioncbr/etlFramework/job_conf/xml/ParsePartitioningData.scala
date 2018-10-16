package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.load.{PartitionColumn, PartitionColumnTypeEnum, PartitioningData}

object ParsePartitioningData {
  def fromXML(node: scala.xml.NodeSeq): PartitioningData = {
    val coalesce = (node \ "coalesce_partition").text.toBoolean
    val overwrite = (node \ "overwrite_partition").text.toBoolean
    val coalesceCount = (node \ "coalesce_partition_count").text.toInt
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
