package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.{DataPath, FileNameParam, PathInfixParam}

class ParseDataPathSpec extends XmlJobConfBase{

  "ParseFileName" should "return FileNameParam object with name params" in {
    val xmlContent: String = "<fileName> <prefix>json_data</prefix> <suffix>json</suffix> </fileName>"
    val FileNameParamObject: FileNameParam = ParseFileName.fromXML(node(xmlContent))
    FileNameParamObject should not equal null

    FileNameParamObject.fileNamePrefix should equal (Some("json_data"))
    FileNameParamObject.fileNamePrefix.get should equal ("json_data")

    FileNameParamObject.fileNameSuffix should equal (Some("json"))
    FileNameParamObject.fileNameSuffix.get should equal ("json")

    FileNameParamObject.fileNameSeparator should equal (Some("."))
  }

  "ParseFileName" should "return FileNameParam object with prefix & suffix as None" in {
    val xmlContent: String = "<fileName> </fileName>"
    val FileNameParamObject: FileNameParam = ParseFileName.fromXML(node(xmlContent))
    FileNameParamObject should not equal null

    FileNameParamObject.fileNamePrefix should equal (None)
    FileNameParamObject.fileNameSuffix should equal (None)
    FileNameParamObject.fileNameSeparator should equal (Some("."))

    val xmlOtherContent: String = "<abc> </abc>"
    val FileNameParamOtherObject: FileNameParam = ParseFileName.fromXML(node(xmlContent))
    FileNameParamOtherObject should not equal null

    FileNameParamOtherObject.fileNamePrefix should equal (None)
    FileNameParamOtherObject.fileNameSuffix should equal (None)
    FileNameParamOtherObject.fileNameSeparator should equal (Some("."))
  }

  "ParseFeedPattern" should "return PathInfixParam object with all variables value" in {
    val xmlContent: String = s"""<feedPattern>
                                |  <feedNamePattern>feed_%s</feedNamePattern>
                                |  <formatFeedName>true</formatFeedName>
                                |  <formatArgValues>
                                |      <param order="1" name="param" value="json" defaultValue="json"/>
                                |  </formatArgValues>
                                |</feedPattern>"""
    val pathInfixParamObject: PathInfixParam = ParseFeedPattern.fromXML(node(xmlContent))

    pathInfixParamObject should not equal null

    pathInfixParamObject.order should be (None)
    pathInfixParamObject.infixPattern should equal ("feed_%s")
    pathInfixParamObject.formatInfix should equal (Some(true))
    pathInfixParamObject.formatInfixArgs.isEmpty should equal (false)
    pathInfixParamObject.formatInfixArgs.get.length should be (1)
    pathInfixParamObject.formatInfixArgs.get.head.order should be(1)
    pathInfixParamObject.formatInfixArgs.get.head.paramDefaultValue should be ("json")
  }

  "ParseFeedPattern" should "return PathInfixParam object with feed_name as only variable with value" in {
    val xmlContent: String = s"""<feedPattern>
                                |  <feedNamePattern>feed_json</feedNamePattern>
                                |</feedPattern>""".stripMargin
    val pathInfixParamObject: PathInfixParam = ParseFeedPattern.fromXML(node(xmlContent))

    pathInfixParamObject should not equal null

    pathInfixParamObject.order should be (None)
    pathInfixParamObject.infixPattern should equal ("feed_json")
    pathInfixParamObject.formatInfix should equal (None)
    pathInfixParamObject.formatInfixArgs.isEmpty should equal (true)
  }

  "ParseGroupPattern" should "return PathInfixParam object with all variables value" in {
    val xmlContent: String = s"""<member>
                                |   <order>1</order>
                                |   <groupNamePattern>group_%s</groupNamePattern>
                                |   <formatGroupName>true</formatGroupName>
                                |   <formatArgValues>
                                |     <param order="1" name="abc" value="group" defaultValue="group"/>
                                |   </formatArgValues>
                                |</member>""".stripMargin
    val pathInfixParamObject: PathInfixParam = ParseGroupPattern.fromXML(node(xmlContent))
    pathInfixParamObject should not equal null
    pathInfixParamObject.order should be (Some(1))
    pathInfixParamObject.infixPattern should equal ("group_%s")
    pathInfixParamObject.formatInfix should equal (Some(true))
    pathInfixParamObject.formatInfixArgs.get.length should be (1)
    pathInfixParamObject.formatInfixArgs.get.head.order should be(1)
    pathInfixParamObject.formatInfixArgs.get.head.paramName should be ("abc")
  }

  "ParseGroupPattern" should "return PathInfixParam object with group_name as only variables with value" in {
    val xmlContent: String = s"""<member>
                                |   <order>1</order>
                                |   <groupNamePattern>group_%s</groupNamePattern>
                                |   <formatGroupName>false</formatGroupName>
                                |</member>""".stripMargin
    val pathInfixParamObject: PathInfixParam = ParseGroupPattern.fromXML(node(xmlContent))
    pathInfixParamObject should not equal null
    pathInfixParamObject.order should be (Some(1))
    pathInfixParamObject.infixPattern should equal ("group_%s")
    pathInfixParamObject.formatInfix should equal (Some(false))
    pathInfixParamObject.formatInfixArgs should be (None)

    val anotherXMLContent: String = s"""<member>
                                |   <order>1</order>
                                |   <groupNamePattern>group_%s</groupNamePattern>
                                |</member>""".stripMargin
    val anotherPathInfixParamObject: PathInfixParam = ParseGroupPattern.fromXML(node(anotherXMLContent))
    anotherPathInfixParamObject should not equal null
    anotherPathInfixParamObject.order should be (Some(1))
    anotherPathInfixParamObject.infixPattern should equal ("group_%s")
    anotherPathInfixParamObject.formatInfix should equal (None)
  }

  "ParseGroupPatterns" should "return Array of PathInfixParam object" in {
    val xmlContent: String = s"""<groupPattern>
                                |   <member>
                                |     <order>1</order>
                                |     <groupNamePattern>group_1</groupNamePattern>
                                |     <formatGroupName>false</formatGroupName>
                                |   </member>
                                |   <member>
                                |     <order>2</order>
                                |     <groupNamePattern>group_2_%s</groupNamePattern>
                                |     <formatGroupName>true</formatGroupName>
                                |     <formatArgValues>
                                |       <param order="1" name="abc" value="group" defaultValue="group"/>
                                |     </formatArgValues>
                                |   </member>
                                |</groupPattern>""".stripMargin
    val pathInfixParamArrayObject: Array[PathInfixParam] = ParseGroupPatterns.fromXML(node(xmlContent))
    pathInfixParamArrayObject should not equal null
    pathInfixParamArrayObject.length should be (2)
    pathInfixParamArrayObject.head.order should be (Some(1))
    pathInfixParamArrayObject.head.infixPattern should be ("group_1")
    pathInfixParamArrayObject.tail.head.order should be (Some(2))
    pathInfixParamArrayObject.tail.head.formatInfixArgs.get.length should be (1)
  }

  "ParseGroupPatterns" should "return Array of PathInfixParam object with zero element" in {
    val xmlContent: String = s"""<groupPattern></groupPattern>""".stripMargin
    val pathInfixParamArrayObject: Array[PathInfixParam] = ParseGroupPatterns.fromXML(node(xmlContent))
    pathInfixParamArrayObject should not equal null
    pathInfixParamArrayObject.length should be (0)
  }

  "ParseDataPath-parsePathPattern" should "return FilePath object with value of all the variables" in {
    val xmlContent = s"""<pathPattern>
          <initialPath>{json-file-path-suffix}</initialPath>
          <groupPattern>
              <member>
                  <order>1</order>
                  <groupNamePattern>group_%s</groupNamePattern>
                  <formatGroupName>true</formatGroupName>
                  <formatArgValues>
                      <param order="1" name="abc" value="group" defaultValue="group"/>
                  </formatArgValues>
              </member>
          </groupPattern>
          <feedPattern>
              <feedNamePattern>feed_%s</feedNamePattern>
              <formatFeedName>true</formatFeedName>
              <formatArgValues>
                  <param order="1" name="xyz" value="json" defaultValue="json"/>
              </formatArgValues>
          </feedPattern>
          <fileName>
              <prefix>json_data</prefix>
              <suffix>json</suffix>
          </fileName>
    </pathPattern>""".stripMargin
    val filePathObject: DataPath = ParseDataPath.parsePathPattern(node(xmlContent))
    filePathObject should not equal null

    filePathObject.pathPrefix should not equal null
    filePathObject.cataloguePatterns should not equal null
    filePathObject.feedPattern should not equal null
    filePathObject.fileName should not equal null

    filePathObject.pathPrefix.get should be ("{json-file-path-suffix}")

    filePathObject.cataloguePatterns.get.length should be (1)
    filePathObject.feedPattern.get.formatInfixArgs should not equal null
    filePathObject.fileName.get.fileNamePrefix should be (Some("json_data"))
  }

  "ParseDataPath-parsePathPattern" should "return FilePath object with only pathPrefix & fileName variable value" in {
    val xmlContent = s"""<pathPattern>
          <initialPath>{json-file-path-suffix}</initialPath>
          <fileName>
              <prefix>json_data</prefix>
              <suffix>json</suffix>
          </fileName>
    </pathPattern>""".stripMargin

    val filePathObject: DataPath = ParseDataPath.parsePathPattern(node(xmlContent))
    filePathObject should not equal null

    filePathObject.pathPrefix should not equal null
    filePathObject.cataloguePatterns should equal (None)
    filePathObject.feedPattern should equal (None)
    filePathObject.fileName should not equal null
  }

  "ParseDataPath" should "return FilePath object parsed from path string" in {
    val path = s"${System.getProperty("user.dir")}/etl_examples/sample_data/json_data.json"
    val xmlContent = s"<dataPath><path>$path</path></dataPath>"
    val filePathObject: DataPath = ParseDataPath.fromXML(node(xmlContent))
    filePathObject should not equal null
    filePathObject.pathPrefix should be (Some(s"${System.getProperty("user.dir")}/etl_examples/sample_data"))
    filePathObject.cataloguePatterns should be (None)
    filePathObject.feedPattern should be (None)
    filePathObject.fileName should not equal null
    filePathObject.fileName.get.fileNamePrefix should be (Some("json_data"))
  }

  "ParseDataPath" should "return FilePath object parsed from pathpatther string" in {
    val xmlContent = s"""<dataPath><pathPattern>
          <initialPath>{json-file-path-suffix}</initialPath>
          <fileName>
              <prefix>json_data</prefix>
              <suffix>json</suffix>
          </fileName>
    </pathPattern></dataPath>""".stripMargin

    val filePathObject: DataPath = ParseDataPath.fromXML(node(xmlContent))
    filePathObject should not equal null
    filePathObject.pathPrefix should be (Some("{json-file-path-suffix}"))
    filePathObject.cataloguePatterns should be (None)
    filePathObject.feedPattern should be (None)
    filePathObject.fileName should not equal null
    filePathObject.fileName.get.fileNamePrefix should be (Some("json_data"))
  }
}
