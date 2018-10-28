package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.extract.{ExtractionType, Feed}

class ParseExtractConfSpec extends XmlJobConfBase {

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
    val feedObject: Feed = ParseFeedConf.fromXML(node(xmlContent))
    feedObject should not equal null
    feedObject.feedName should be ("json_data_feed")
    feedObject.validateExtractedData should be (false)
    feedObject.extractionType should be (ExtractionType.FILE_SYSTEM)

    feedObject.dataPath.isDefined should be (true)
    feedObject.dataPath.get.pathPrefix should be (Some("{json-file-path-suffix}"))

    feedObject.query.isDefined should be (false)
    feedObject.query should be (None)
  }

  "ParseFeedConf" should "return Feed object with query variable" in {
    val xmlContent = s"""<feed feedName="feed1" validateExtractedData="true">
                        |  <jdbc type="DB">
                        |     <query>
                        |        <sqlQueryFile><path>{sql-query-file-path.sql}</path></sqlQueryFile>
                        |        <configurationFile><path>{db-property-file-path}</path></configurationFile>
                        |        <queryParams><param order="1" name="{col1}" value="FIRST_DATE" /></queryParams>
                        |     </query>
                        |  </jdbc>
                        |</feed>""".stripMargin
    val feedObject: Feed = ParseFeedConf.fromXML(node(xmlContent))
    feedObject should not equal null
    feedObject.feedName should be ("feed1")
    feedObject.validateExtractedData should be (true)
    feedObject.extractionType should be (ExtractionType.DB)

    feedObject.dataPath.isDefined should be (false)

    feedObject.query.isDefined should be (true)
    feedObject.query.get.queryFile should not equal null
  }
}
