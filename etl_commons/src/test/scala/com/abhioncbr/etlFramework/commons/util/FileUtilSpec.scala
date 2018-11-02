package com.abhioncbr.etlFramework.commons.util

import com.abhioncbr.etlFramework.commons.{CommonSpec, Fixture}

class FileUtilSpec extends CommonSpec {

  "getFilePathString" should "return only pathPrefix as path string" in {
    val expectedPath: String = Fixture.pathPrefix + "/"
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath)
    pathString should not be null
    pathString should be (expectedPath)
  }

  "getFilePathString" should "return pathPrefix & catalogue as path string" in {
    val expectedPath: String = Fixture.pathPrefix + "/" + Fixture.catalogueStaticInfixPattern1 + "/"
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath1)
    pathString should not be null
    pathString should be (expectedPath)
  }

  "getFilePathString" should "return pathPrefix, catalogue & feed as path string" in {
    val expectedPath: String = Fixture.pathPrefix + "/" + Fixture.catalogueStaticInfixPattern1 + "/" + Fixture.feedStaticInfixParam + "/"
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath2)
    pathString should not be null
    pathString should be (expectedPath)
  }

  "getFilePathString" should "return pathPrefix, catalogue, feed and fileName as path string" in {
    val expectedPath: String = Fixture.pathPrefix + "/" + Fixture.catalogueStaticInfixPattern1 + "/" + Fixture.feedStaticInfixParam + "/" + Fixture.fileNamePrefix1 + "." + Fixture.fileNameSuffix1
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath3)
    pathString should not be null
    println(expectedPath)
    pathString should be (expectedPath)
  }

}
