package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.extract.{ExtractConf, ExtractionType, ExtractFeedConf}

class ParseExtractSpec extends XmlJobConfBase {

  "ParseFeedConf" should "return Feed object with dataPath variable" in {
    val xmlContent = s""" <feed feedName="json_data_feed" validateExtractedData="false">
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
        </feed>""".stripMargin
    val feedObject: ExtractFeedConf = ParseExtractFeed.fromXML(node(xmlContent))
    feedObject should not equal null
    feedObject.extractFeedName should be ("json_data_feed")
    feedObject.validateExtractedData should be (false)
    feedObject.extractionType should be (ExtractionType.FILE_SYSTEM)

    feedObject.dataPath.isDefined should be (true)
    feedObject.dataPath.get.pathPrefix should be (Some("{json-file-path-suffix}"))

    feedObject.query.isDefined should be (false)
    feedObject.query should be (None)
  }

  "ParseFeedConf" should "return Feed object with query variable" in {
    val xmlContent = s"""<feed feedName="feed1" validateExtractedData="true">
                        |  <jdbc databaseType="DB">
                        |     <query>
                        |        <sqlQueryFile><path>sql-query-file-path.sql</path></sqlQueryFile>
                        |        <configurationFile><path>db-property-file-path.properties</path></configurationFile>
                        |        <queryParams><param order="1" name="{col1}" value="FIRST_DATE" /></queryParams>
                        |     </query>
                        |  </jdbc>
                        |</feed>""".stripMargin
    val feedObject: ExtractFeedConf = ParseExtractFeed.fromXML(node(xmlContent))
    feedObject should not equal null
    feedObject.extractFeedName should be ("feed1")
    feedObject.validateExtractedData should be (true)
    feedObject.extractionType should be (ExtractionType.JDBC)

    feedObject.dataPath.isDefined should be (false)

    feedObject.query.isDefined should be (true)
    feedObject.query.get.queryFile should not equal null
    feedObject.query.get.queryArgs.get.length should be (1)
    feedObject.query.get.queryArgs.get.head.paramName should be ("{col1}")

    //Should return 'null' since sql-queryFile & db-propertyFile is not present
    feedObject.query.get.queryFile.queryFile should be (Some(null))
    feedObject.query.get.queryFile.configurationFile should be (Some(null))
  }

  "ParseExtractConf" should "return Extract object with feed array object" in {
    val xmlContent = s"""<extract>
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
    extractObject should not equal null
    extractObject.feeds should not equal null
    extractObject.feeds.length should be (2)

    extractObject.feeds.head.extractFeedName should be ("feed1")
    extractObject.feeds.head.extractionType should be (ExtractionType.JDBC)

    extractObject.feeds.tail.head.extractFeedName should be ("feed2")
    extractObject.feeds.tail.head.validateExtractedData should be (false)
  }
}
