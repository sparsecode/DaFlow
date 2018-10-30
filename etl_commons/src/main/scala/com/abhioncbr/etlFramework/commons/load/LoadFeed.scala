package com.abhioncbr.etlFramework.commons.load

import com.abhioncbr.etlFramework.commons.common.file.FilePath

case class Load(feeds: Array[LoadFeed])
case class LoadFeed(loadFeedName: String,
                    loadType: LoadType.valueType, attributesMap: Map[String,String],
                    dataPath: FilePath, partitioningData: Option[PartitioningData] )
