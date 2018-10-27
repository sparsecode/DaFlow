package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.common.query.{QueryObject, QueryFilesParam}

object ParseQuery {
  def fromXML(node: scala.xml.NodeSeq): Option[QueryObject] = {
    val configurationFile: Option[FilePath] = ParseDataPath.fromXML(node \ "configurationFile")
    val queryFile: Option[FilePath] = ParseDataPath.fromXML(node \ "sqlQueryFile")
    val queryArgs: Option[Array[GeneralParam]] = ParseUtil.parseNode[Array[GeneralParam]](node \ "queryParams",None, ParseGeneralParams.fromXML) //Some(ParseGeneralParams.fromXML(node, nodeTag= "queryParams"))

    if(configurationFile.isEmpty && queryFile.isEmpty) None
    else {
      val queryFiles: QueryFilesParam = QueryFilesParam(configurationFile = configurationFile, queryFile = queryFile)
      val query: QueryObject = QueryObject(queryFile = queryFiles, queryArgs = queryArgs)
      Some(query)
    }
  }
}
