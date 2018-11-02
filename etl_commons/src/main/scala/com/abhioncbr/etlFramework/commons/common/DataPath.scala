package com.abhioncbr.etlFramework.commons.common

case class DataPath(pathPrefix: Option[String], cataloguePatterns: Option[Array[PathInfixParam]] = None, feedPattern: Option[PathInfixParam] = None, fileName: Option[FileNameParam] = None)
case class PathInfixParam(order: Option[Int] = None, infixPattern: String, formatInfix: Option[Boolean] = Some(false), formatInfixArgs: Option[Array[GeneralParamConf]] =None)
case class FileNameParam(fileNamePrefix: Option[String] = None, fileNameSuffix: Option[String] = None, fileNameSeparator: Option[String]= Some("."))
