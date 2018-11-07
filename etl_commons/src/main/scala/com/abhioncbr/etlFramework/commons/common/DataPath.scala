/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.abhioncbr.etlFramework.commons.common

case class DataPath(pathPrefix: Option[String], cataloguePatterns: Option[Array[PathInfixParam]] = None,
  feedPattern: Option[PathInfixParam] = None, fileName: Option[FileNameParam] = None)

case class PathInfixParam(order: Option[Int] = None, infixPattern: String,
  formatInfix: Option[Boolean] = Some(false), formatInfixArgs: Option[Array[GeneralParamConf]] = None)

case class FileNameParam(fileNamePrefix: Option[String] = None, fileNameSuffix: Option[String] = None,
  fileNameSeparator: Option[String] = Some("."))
