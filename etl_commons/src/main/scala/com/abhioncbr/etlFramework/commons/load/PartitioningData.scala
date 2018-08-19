package com.abhioncbr.etlFramework.commons.load

case class PartitioningData(coalesce: Boolean, overwrite: Boolean, coalesceCount:Int, partitionColumns: List[PartitionColumn], partitionFileInitialPath: String)
