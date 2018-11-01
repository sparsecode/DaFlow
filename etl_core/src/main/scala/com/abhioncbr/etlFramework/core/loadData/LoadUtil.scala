package com.abhioncbr.etlFramework.core.loadData

import com.abhioncbr.etlFramework.commons.load.PartitioningDataConf

object LoadUtil {
  def getPartitioningString(data: PartitioningDataConf): String ={
    data.partitionColumns.map(col => s"${col.paramName} = '${col.paramValue}'").mkString(" , ")
  }
}
