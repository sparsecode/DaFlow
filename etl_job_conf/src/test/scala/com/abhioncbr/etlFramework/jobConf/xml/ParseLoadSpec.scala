package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.load.LoadFeed

class ParseLoadSpec extends XmlJobConfBase {

  "ParseLoadFeed" should "return LoadFeed object with all hive based variables" in {
    val path = s"${System.getProperty("user.dir")}/etl_examples/sample_data/"
    val xmlContent = s"""<feed name="feed1">
            <hive dataBaseName="{db-name}" tableName="{table-name}" fileType="PARQUET">
                <partitionData coalescePartition="true" overwritePartition="true" coalescePartitionCount="10">
                    <partitionColumns>
                        <column name="" value=""/>
                    </partitionColumns>
                </partitionData>
                <dataPath>
                    <path>$path</path>
                </dataPath>
            </hive>
        </feed>"""
    val parseLoadFeedObject: LoadFeed = ParseLoadFeed.fromXML(node(xmlContent))
    parseLoadFeedObject should not be null
    parseLoadFeedObject.loadFeedName should be ("feed1")
    parseLoadFeedObject.attributesMap.size should be (3)
    parseLoadFeedObject.attributesMap("dataBaseName") should be ("{db-name}")
    parseLoadFeedObject.attributesMap("tableName") should be ("{table-name}")
    parseLoadFeedObject.attributesMap("fileType") should be ("PARQUET")
    parseLoadFeedObject.partitioningData should not be null
    parseLoadFeedObject.partitioningData.get.coalesce should be (true)
    parseLoadFeedObject.dataPath should not be null
    parseLoadFeedObject.dataPath.pathPrefix should be (Some(s"${System.getProperty("user.dir")}/etl_examples"))
    parseLoadFeedObject.dataPath.feedPattern should not be null
    parseLoadFeedObject.dataPath.feedPattern.get.infixPattern should be ("sample_data")
  }

  "ParseLoadFeed" should "return LoadFeed object with all fileSystem based variables" in {
    val path = s"${System.getProperty("user.dir")}/etl_examples/sample_data/"
    val xmlContent = s"""<feed name="json_data">
                            <fileSystem fileType="JSON">
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
    val parseLoadFeedObject: LoadFeed = ParseLoadFeed.fromXML(node(xmlContent))
    parseLoadFeedObject should not be null
    parseLoadFeedObject.loadFeedName should be ("json_data")
    parseLoadFeedObject.attributesMap.size should be (1)
    parseLoadFeedObject.attributesMap("fileType") should be ("JSON")
    parseLoadFeedObject.partitioningData should be (None)
    parseLoadFeedObject.dataPath should not be null
    parseLoadFeedObject.dataPath.pathPrefix should be (Some(s"{json-file-path-suffix}"))
    parseLoadFeedObject.dataPath.fileName.get.fileNamePrefix should be (Some("json_data"))
    parseLoadFeedObject.dataPath.fileName.get.fileNameSuffix should be (Some("json"))

  }
}
