package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.common.query.{QueryObject, QueryFilesParam}

object ParseQuery {
  def fromXML(node: scala.xml.NodeSeq): QueryObject = {
    val configurationFile: Option[FilePath] = ParseUtil.parseNode[FilePath](node \ "configurationFile", None, ParseDataPath.fromXML)
    val queryFile: Option[FilePath] = ParseUtil.parseNode[FilePath](node \ "sqlQueryFile", None, ParseDataPath.fromXML)
    val queryArgs: Option[Array[GeneralParam]] = ParseUtil.parseNode[Array[GeneralParam]](node \ "queryParams", None, ParseGeneralParams.fromXML) //Some(ParseGeneralParams.fromXML(node, nodeTag= "queryParams"))

    val queryFiles: QueryFilesParam = QueryFilesParam(configurationFile = configurationFile, queryFile = queryFile)
    val query: QueryObject = QueryObject(queryFile = queryFiles, queryArgs = queryArgs)
    query
  }
}
