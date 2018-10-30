package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.load.{Load, LoadFeed, LoadType, PartitioningData}

object ParseLoad {
  def fromXML(node: scala.xml.NodeSeq): Load = {val load: Load = Load(feeds = Array[LoadFeed]((node \ "feed" ).toList map { s => ParseLoadFeed.fromXML(s) }: _*))
    load
  }
}

object ParseLoadFeed {
  def fromXML(node: scala.xml.NodeSeq): LoadFeed = {
    val loadFeedName : String = (node \ "@feedName").text
    val loadType: LoadType.valueType = LoadType.getValueType( valueTypeString = (node \ "_").head.label.toUpperCase)
    val attributesMap: Map[String, String] = (node \ "_").head.attributes.map(meta => (meta.key, meta.value.toString)).toMap
    val dataPath: FilePath = ParseUtil.parseNode[FilePath](node \ "_" \ "dataPath", None, ParseDataPath.fromXML).orNull
    val partitioningData: Option[PartitioningData] = ParseUtil.parseNode[PartitioningData](node \ "hive" \"partition_data", None, ParsePartitioningData.fromXML)

    val feed: LoadFeed = LoadFeed(loadFeedName = loadFeedName, loadType = loadType, attributesMap = attributesMap, dataPath = dataPath, partitioningData = partitioningData)
    feed
  }
}

