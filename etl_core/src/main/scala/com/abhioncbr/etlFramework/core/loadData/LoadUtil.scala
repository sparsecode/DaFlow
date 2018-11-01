package com.abhioncbr.etlFramework.core.loadData

import com.abhioncbr.etlFramework.commons.load.PartitioningData

object LoadUtil {
  def getPartitioningString(data: PartitioningData): String ={
    data.partitionColumns.map(col => s"${col.paramName} = '${col.paramValue}'").mkString(" , ")
  }
}
