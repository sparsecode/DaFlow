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

import com.abhioncbr.daflow.commons.conf.load.LoadFeedConf

class ParseLoadSpec extends XmlJobConfBase {

  "ParseLoadFeed" should "return LoadFeed object with all hive based variables" in {
    val path = s"$daflowExamplesDemoSampleDataPath/"
    val xmlContent = s"""<feed name="feed1">
            <hive dataBaseName="{db-name}" tableName="{table-name}" fileType="PARQUET">
                <partitionData coalescePartition="true" overwritePartition="true" coalescePartitionCount="10">
                    <partitionColumns>
                        <column name="Date" value="date"/>
                    </partitionColumns>
                </partitionData>
                <dataPath>
                  <path>$path</path>
                </dataPath>
            </hive>
        </feed>"""
    val parseLoadFeedObject: LoadFeedConf = ParseLoadFeed.fromXML(node(xmlContent))
    parseLoadFeedObject should not be None
    parseLoadFeedObject.loadFeedName should be ("feed1")
    parseLoadFeedObject.attributesMap.size should be (3)
    parseLoadFeedObject.attributesMap("dataBaseName") should be ("{db-name}")
    parseLoadFeedObject.attributesMap("tableName") should be ("{table-name}")
    parseLoadFeedObject.attributesMap("fileType") should be ("PARQUET")
    parseLoadFeedObject.partitioningData should not be None
    parseLoadFeedObject.partitioningData.get.coalesce should be (true)
    parseLoadFeedObject.dataPath should not be None
    parseLoadFeedObject.dataPath.pathPrefix should be (Some(daflowExamplesDemoPath))
    parseLoadFeedObject.dataPath.feedPattern should not be None
    parseLoadFeedObject.dataPath.feedPattern.get.infixPattern should be ("sample-data")
  }

  "ParseLoadFeed" should "return LoadFeed object with all fileSystem based variables" in {
    val xmlContent = """<feed name="json_data">
                            <fileSystem fileType="JSON" isPathRelative="true">
                              <dataPath>
                                <pathPattern>
                                  <initialPath>{json-file-path-suffix}</initialPath>
                                  <fileName>
                                    <prefix>json_data</prefix>
                                    <suffix>json</suffix>
                                  </fileName>
                                </pathPattern>
                              </dataPath>
                            </fileSystem>
                        </feed>"""
    val parseLoadFeedObject: LoadFeedConf = ParseLoadFeed.fromXML(node(xmlContent))
    parseLoadFeedObject should not be None
    parseLoadFeedObject.loadFeedName should be ("json_data")
    parseLoadFeedObject.attributesMap.size should be (2)
    parseLoadFeedObject.attributesMap("fileType") should be ("JSON")
    parseLoadFeedObject.attributesMap("isPathRelative") should be ("true")
    parseLoadFeedObject.partitioningData should be (None)
    parseLoadFeedObject.dataPath should not be None
    parseLoadFeedObject.dataPath.pathPrefix should be (Some("{json-file-path-suffix}"))
    parseLoadFeedObject.dataPath.fileName.get.fileNamePrefix should be (Some("json_data"))
    parseLoadFeedObject.dataPath.fileName.get.fileNameSuffix should be (Some("json"))
  }
}
