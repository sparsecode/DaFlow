package com.abhioncbr.etlFramework.commons.load

import com.abhioncbr.etlFramework.commons.common.file.FilePath

case class PartitioningData(coalesce: Boolean, overwrite: Boolean, coalesceCount:Int, partitionColumns: List[PartitionColumn], dataPath: FilePath)
