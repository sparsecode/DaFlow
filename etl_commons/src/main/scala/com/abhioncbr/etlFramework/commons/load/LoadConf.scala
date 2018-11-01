package com.abhioncbr.etlFramework.commons.load

import com.abhioncbr.etlFramework.commons.common.DataPath

case class LoadConf(feeds: Array[LoadFeedConf])
case class LoadFeedConf(loadFeedName: String,
                        loadType: LoadType.valueType, attributesMap: Map[String,String],
                        dataPath: DataPath, partitioningData: Option[PartitioningDataConf] )
