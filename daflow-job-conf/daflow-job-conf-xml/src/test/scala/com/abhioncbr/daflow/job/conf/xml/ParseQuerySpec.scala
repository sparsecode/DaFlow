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

package com.abhioncbr.daflow.job.conf.xml

import com.abhioncbr.daflow.commons.conf.common.QueryConf

class ParseQuerySpec extends XmlJobConfBase {

  "ParseQuery" should "return QueryObject object with all variables initialised" in {
    val xmlContent = """<query>
                            <sqlQueryFile><path>{sql-query-file-path.sql}</path></sqlQueryFile>
                            <configurationFile><path>{db-property-file-path}</path></configurationFile>
                            <queryParams><param order="1" name="{col1}" value="FIRST_DATE"  /></queryParams>
                          </query>"""
    val queryObject: QueryConf = ParseQuery.fromXML(node(xmlContent))
    queryObject should not equal None
    queryObject.queryFile should not equal None
    queryObject.queryFile.configurationFile.isDefined should be (true)
    queryObject.queryFile.queryFile.isDefined should be (true)
    queryObject.queryArgs.isDefined should be (true)
    queryObject.queryArgs.get.length should be (1)
  }

  "ParseQuery" should "return QueryObject object with sqlQueryFile & queryParams initialised" in {
    val xmlContent = """<query>
                            <sqlQueryFile>
                              <pathPattern>
                                  <initialPath>{initial-path}</initialPath>
                                  <fileName>
                                    <prefix>query_file</prefix>
                                    <suffix>sql</suffix>
                                  </fileName>
                              </pathPattern>
                            </sqlQueryFile>
                            <queryParams><param order="1" name="{col1}" value="FIRST_DATE"/></queryParams>
                          </query>"""
    val queryObject: QueryConf = ParseQuery.fromXML(node(xmlContent))
    queryObject should not equal None
    queryObject.queryFile should not equal None
    queryObject.queryFile.configurationFile.isDefined should be (false)
    queryObject.queryFile.queryFile.isDefined should be (true)
    queryObject.queryFile.queryFile.get.pathPrefix should be (Some("{initial-path}"))
    queryObject.queryFile.queryFile.get.feedPattern should be (None)
    queryObject.queryFile.queryFile.get.fileName.get.fileNamePrefix should be (Some("query_file"))
    queryObject.queryFile.queryFile.get.fileName.get.fileNameSuffix should be (Some("sql"))
    queryObject.queryArgs.isDefined should be (true)
    queryObject.queryArgs.get.length should be (1)
    queryObject.queryArgs.get.head.paramName should be ("{col1}")
  }

  "ParseQuery" should "return QueryObject object with only sqlQueryFile initialised" in {
    val xmlContent = """<query>
                            <sqlQueryFile>
                              <pathPattern>
                                  <initialPath>{initial-path}</initialPath>
                                  <fileName>
                                    <prefix>query_file</prefix>
                                    <suffix>sql</suffix>
                                  </fileName>
                              </pathPattern>
                            </sqlQueryFile>
                          </query>"""
    val queryObject: QueryConf = ParseQuery.fromXML(node(xmlContent))
    queryObject should not equal None
    queryObject.queryFile should not equal None
    queryObject.queryFile.configurationFile.isDefined should be (false)
    queryObject.queryFile.queryFile.isDefined should be (true)
    queryObject.queryFile.queryFile.get.pathPrefix should be (Some("{initial-path}"))
    queryObject.queryFile.queryFile.get.feedPattern should be (None)
    queryObject.queryFile.queryFile.get.fileName.get.fileNamePrefix should be (Some("query_file"))
    queryObject.queryFile.queryFile.get.fileName.get.fileNameSuffix should be (Some("sql"))
    queryObject.queryArgs.isDefined should be (false)
  }
}
