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

package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.DataPath
import com.abhioncbr.etlFramework.commons.common.GeneralParamConf
import com.abhioncbr.etlFramework.commons.common.QueryConf
import com.abhioncbr.etlFramework.commons.common.QueryFilesConf

object ParseQuery {
  def fromXML(node: scala.xml.NodeSeq): QueryConf = {
    val configurationFile: Option[DataPath] =
      ParseUtil.parseNode[DataPath](node \ "configurationFile", None, ParseDataPath.fromXML)

    val queryFile: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ "sqlQueryFile", None, ParseDataPath.fromXML)

    val queryArgs: Option[Array[GeneralParamConf]] =
      ParseUtil.parseNode[Array[GeneralParamConf]](node \ "queryParams", None, ParseGeneralParams.fromXML)

    val queryFiles: QueryFilesConf = QueryFilesConf(configurationFile = configurationFile, queryFile = queryFile)
    val query: QueryConf = QueryConf(queryFile = queryFiles, queryArgs = queryArgs)
    query
  }
}
