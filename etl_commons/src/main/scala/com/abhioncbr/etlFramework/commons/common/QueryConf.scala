package com.abhioncbr.etlFramework.commons.common

case class QueryConf(queryFile: QueryFilesConf, queryArgs: Option[Array[GeneralParamConf]])
case class QueryFilesConf(configurationFile: Option[DataPath], queryFile: Option[DataPath])
