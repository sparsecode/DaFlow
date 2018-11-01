package com.abhioncbr.etlFramework.commons.load

import com.abhioncbr.etlFramework.commons.common.GeneralParamConf

case class PartitioningDataConf(coalesce: Boolean, overwrite: Boolean, coalesceCount:Int, partitionColumns: List[GeneralParamConf])
