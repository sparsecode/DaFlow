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

package com.abhioncbr.daflow.jobConf.xml

import com.abhioncbr.daflow.commons.extract.ExtractConf
import com.abhioncbr.daflow.commons.extract.ExtractFeedConf
import com.abhioncbr.daflow.commons.extract.ExtractionType

class ParseExtractSpec extends XmlJobConfBase {

  "ParseFeedConf" should "return Feed object with dataPath variable" in {
    val xmlContent = """ <feed feedName="json_data_feed" validateExtractedData="false">
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
        </feed>""".stripMargin
    val feedObject: ExtractFeedConf = ParseExtractFeed.fromXML(node(xmlContent))
    feedObject should not equal None
    feedObject.extractFeedName should be ("json_data_feed")
    feedObject.validateExtractedData should be (false)
    feedObject.extractionType should be (ExtractionType.FILE_SYSTEM)
    feedObject.extractionAttributesMap.size should be (2)
    feedObject.extractionAttributesMap("fileType") should be ("JSON")
    feedObject.extractionAttributesMap("isPathRelative") should be ("true")

    feedObject.dataPath.isDefined should be (true)
    feedObject.dataPath.get.pathPrefix should be (Some("{json-file-path-suffix}"))

    feedObject.query.isDefined should be (false)
    feedObject.query should be (None)
  }

  "ParseFeedConf" should "return Feed object with query variable" in {
    val xmlContent = """<feed feedName="feed1" validateExtractedData="true">
                        |  <jdbc databaseType="DB">
                        |     <query>
                        |        <sqlQueryFile><path>sql-query-file-path.sql</path></sqlQueryFile>
                        |        <configurationFile><path>db-property-file-path.properties</path></configurationFile>
                        |        <queryParams><param order="1" name="{col1}" value="FIRST_DATE" /></queryParams>
                        |     </query>
                        |  </jdbc>
                        |</feed>""".stripMargin
    val feedObject: ExtractFeedConf = ParseExtractFeed.fromXML(node(xmlContent))
    feedObject should not equal None
    feedObject.extractFeedName should be ("feed1")
    feedObject.validateExtractedData should be (true)
    feedObject.extractionType should be (ExtractionType.JDBC)

    feedObject.dataPath.isDefined should be (false)

    feedObject.query.isDefined should be (true)
    feedObject.query.get.queryFile should not equal None
    feedObject.query.get.queryArgs.get.length should be (1)
    feedObject.query.get.queryArgs.get.head.paramName should be ("{col1}")

    // Should return 'null' since sql-queryFile & db-propertyFile is not present
    feedObject.query.get.queryFile.queryFile should be (Some(null))
    feedObject.query.get.queryFile.configurationFile should be (Some(null))
  }

  "ParseExtractConf" should "return Extract object with feed array object" in {
    val xmlContent = """<extract>
                        | <feed feedName="feed1" validateExtractedData="true">
                        |  <jdbc databaseType="DB">
                        |     <query>
                        |        <sqlQueryFile><path>sql-query-file-path.sql</path></sqlQueryFile>
                        |        <configurationFile><path>db-property-file-path.properties</path></configurationFile>
                        |        <queryParams><param order="1" name="{col1}" value="FIRST_DATE" /></queryParams>
                        |     </query>
                        |  </jdbc>
                        | </feed>
                        | <feed feedName="feed2" validateExtractedData="false">
                        |  <fileSystem fileType="JSON">
                        |    <dataPath>
                        |     <pathPattern>
                        |       <initialPath>{json-file-path-suffix}</initialPath>
                        |         <fileName>
                        |           <prefix>json_data</prefix>
                        |           <suffix>json</suffix>
                        |         </fileName>
                        |     </pathPattern>
                        |   </dataPath>
                        |  </fileSystem>
                        | </feed>
                        |</extract>""".stripMargin
    val extractObject: ExtractConf = ParseExtract.fromXML(node(xmlContent))
    extractObject should not equal None
    extractObject.feeds should not equal None
    extractObject.feeds.length should be (2)

    extractObject.feeds.head.extractFeedName should be ("feed1")
    extractObject.feeds.head.extractionType should be (ExtractionType.JDBC)

    extractObject.feeds.tail.head.extractFeedName should be ("feed2")
    extractObject.feeds.tail.head.validateExtractedData should be (false)
  }
}
