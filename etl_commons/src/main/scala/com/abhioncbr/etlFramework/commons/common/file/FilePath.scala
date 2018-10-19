package com.abhioncbr.etlFramework.commons.common.file

import com.abhioncbr.etlFramework.commons.common.GeneralParam

case class FilePath(pathPrefix: Option[String], groupPatterns: Option[Array[PathInfixParam]] = None, feedPattern: Option[PathInfixParam] = None, fileName: Option[FileNameParam] = None)
case class PathInfixParam(infixPattern: String, formatInfix: Option[Boolean] = Some(false), formatInfixArgs: Option[Array[GeneralParam]] =None)
case class FileNameParam(fileNamePrefix: Option[String] = None, fileNameSuffix: Option[String] = None, fileNameSeparator: Option[String]= Some("."))
