package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.load.{Load, LoadType}

object ParseLoad {
  def fromXML(node: scala.xml.NodeSeq): Load = {
    Load(subTask = (node \ "task").text,
      loadType = LoadType.getValueType((node \ "type").text),
      dbName = (node \ "db_name").text, tableName = (node \ "table_name").text,
      datasetName= (node \ "dataset").text, feedName= (node \ "feed_name").text,
      dataPath = FilePath(pathPrefix = Some((node \ "file_initial_path").text),
        groupPatterns = None,
        feedPattern = None, //Some(PathInfixParam((node \ "file_name_pattern").text, Some((node \ "format_file_name").text.toBoolean))),
        fileName = None //Some(FileNameParam(Some((node \ "file_prefix").text)))
      ), //(node \ "file_initial_path").text,
      fileType = (node \ "file_type").text, partData = ParsePartitioningData.fromXML(node \ "partition_data"))
  }
}