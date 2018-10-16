package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.load.{Load, LoadType}

object ParseLoad {
  def fromXML(node: scala.xml.NodeSeq): Load = {
    Load(subTask = (node \ "task").text, loadType = LoadType.getValueType((node \ "type").text), dbName = (node \ "db_name").text,
      tableName = (node \ "table_name").text, datasetName= (node \ "dataset").text, feedName= (node \ "feed_name").text, fileInitialPath= (node \ "file_initial_path").text,
      fileType = (node \ "file_type").text, partData = ParsePartitioningData.fromXML(node \ "partition_data"))
  }
}