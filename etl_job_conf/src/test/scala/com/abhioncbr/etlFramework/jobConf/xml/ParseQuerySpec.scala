package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.QueryObject

class ParseQuerySpec extends XmlJobConfBase {

  "ParseQuery" should "return QueryObject object with all variables initialised" in {
    val xmlContent = s"""<query>
                            <sqlQueryFile><path>{sql-query-file-path.sql}</path></sqlQueryFile>
                            <configurationFile><path>{db-property-file-path}</path></configurationFile>
                            <queryParams><param order="1" name="{col1}" value="FIRST_DATE"  /></queryParams>
                          </query>"""
    val queryObject: QueryObject = ParseQuery.fromXML(node(xmlContent))
    queryObject should not equal null
    queryObject.queryFile should not equal null
    queryObject.queryFile.configurationFile.isDefined should be (true)
    queryObject.queryFile.queryFile.isDefined should be (true)
    queryObject.queryArgs.isDefined should be (true)
    queryObject.queryArgs.get.length should be (1)
  }

  "ParseQuery" should "return QueryObject object with sqlQueryFile & queryParams initialised" in {
    val xmlContent = s"""<query>
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
    val queryObject: QueryObject = ParseQuery.fromXML(node(xmlContent))
    queryObject should not equal null
    queryObject.queryFile should not equal null
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
    val xmlContent = s"""<query>
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
    val queryObject: QueryObject = ParseQuery.fromXML(node(xmlContent))
    queryObject should not equal null
    queryObject.queryFile should not equal null
    queryObject.queryFile.configurationFile.isDefined should be (false)
    queryObject.queryFile.queryFile.isDefined should be (true)
    queryObject.queryFile.queryFile.get.pathPrefix should be (Some("{initial-path}"))
    queryObject.queryFile.queryFile.get.feedPattern should be (None)
    queryObject.queryFile.queryFile.get.fileName.get.fileNamePrefix should be (Some("query_file"))
    queryObject.queryFile.queryFile.get.fileName.get.fileNameSuffix should be (Some("sql"))
    queryObject.queryArgs.isDefined should be (false)
  }
}
