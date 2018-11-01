package com.abhioncbr.etlFramework.commons.common

import com.abhioncbr.etlFramework.commons.common.file.DataPath

case class QueryObject(queryFile: QueryFilesParam, queryArgs: Option[Array[GeneralParam]])
case class QueryFilesParam(configurationFile: Option[DataPath], queryFile: Option[DataPath])
