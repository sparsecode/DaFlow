package com.abhioncbr.etlFramework.commons.common.file

case class FilePath(pathPrefix: Option[String], groupPatterns: Option[Array[PathInfixParam]] = None, feedPattern: Option[PathInfixParam] = None, fileName: FileNameParam)
case class PathInfixParam(infixPattern: String, formatInfix: Option[Boolean] = Some(false), formatInfixArgs: Option[Array[String]] =None)
case class FileNameParam(fileNamePrefix: Option[String] = None, fileNameSuffix: Option[String] = None, fileNameSeparator: Option[String]= Some("."))
