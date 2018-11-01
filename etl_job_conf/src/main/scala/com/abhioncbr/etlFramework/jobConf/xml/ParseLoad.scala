package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.DataPath
import com.abhioncbr.etlFramework.commons.load.{LoadConf, LoadFeedConf, LoadType, PartitioningDataConf}

object ParseLoad {
  def fromXML(node: scala.xml.NodeSeq): LoadConf = {val load: LoadConf = LoadConf(feeds = Array[LoadFeedConf]((node \ "feed" ).toList map { s => ParseLoadFeed.fromXML(s) }: _*))
    load
  }
}

object ParseLoadFeed {
  def fromXML(node: scala.xml.NodeSeq): LoadFeedConf = {
    val loadFeedName : String = (node \ "@name").text
    val loadType: LoadType.valueType = LoadType.getValueType( valueTypeString = (node \ "_").head.label.toUpperCase)
    val attributesMap: Map[String, String] = (node \ "_").head.attributes.map(meta => (meta.key, meta.value.toString)).toMap
    val dataPath: DataPath = ParseUtil.parseNode[DataPath](node \ "_" \ "dataPath", None, ParseDataPath.fromXML).orNull
    val partitioningData: Option[PartitioningDataConf] = ParseUtil.parseNode[PartitioningDataConf](node \ "hive" \"partitionData", None, ParsePartitioningData.fromXML)

    val feed: LoadFeedConf = LoadFeedConf(loadFeedName = loadFeedName, loadType = loadType, attributesMap = attributesMap, dataPath = dataPath, partitioningData = partitioningData)
    feed
  }
}

