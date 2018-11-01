package com.abhioncbr.etlFramework.commons.load

import com.abhioncbr.etlFramework.commons.common.GeneralParam

case class PartitioningData(coalesce: Boolean, overwrite: Boolean, coalesceCount:Int, partitionColumns: List[GeneralParam])
