package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.{GeneralParam, QueryFilesParam}
import com.abhioncbr.etlFramework.commons.common.file.DataPath
import com.abhioncbr.etlFramework.commons.common.QueryObject

object ParseQuery {
  def fromXML(node: scala.xml.NodeSeq): QueryObject = {
    val configurationFile: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ "configurationFile", None, ParseDataPath.fromXML)
    val queryFile: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ "sqlQueryFile", None, ParseDataPath.fromXML)
    val queryArgs: Option[Array[GeneralParam]] = ParseUtil.parseNode[Array[GeneralParam]](node \ "queryParams", None, ParseGeneralParams.fromXML) //Some(ParseGeneralParams.fromXML(node, nodeTag= "queryParams"))

    val queryFiles: QueryFilesParam = QueryFilesParam(configurationFile = configurationFile, queryFile = queryFile)
    val query: QueryObject = QueryObject(queryFile = queryFiles, queryArgs = queryArgs)
    query
  }
}
