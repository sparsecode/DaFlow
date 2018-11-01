package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.{DataPath, GeneralParamConf, QueryFilesConf, QueryConf}

object ParseQuery {
  def fromXML(node: scala.xml.NodeSeq): QueryConf = {
    val configurationFile: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ "configurationFile", None, ParseDataPath.fromXML)
    val queryFile: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ "sqlQueryFile", None, ParseDataPath.fromXML)
    val queryArgs: Option[Array[GeneralParamConf]] = ParseUtil.parseNode[Array[GeneralParamConf]](node \ "queryParams", None, ParseGeneralParams.fromXML) //Some(ParseGeneralParams.fromXML(node, nodeTag= "queryParams"))

    val queryFiles: QueryFilesConf = QueryFilesConf(configurationFile = configurationFile, queryFile = queryFile)
    val query: QueryConf = QueryConf(queryFile = queryFiles, queryArgs = queryArgs)
    query
  }
}
