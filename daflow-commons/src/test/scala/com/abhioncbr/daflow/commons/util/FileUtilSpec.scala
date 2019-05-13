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

package com.abhioncbr.daflow.commons.util

import com.abhioncbr.daflow.commons.CommonConstants.{DIRECTORY_SEPARATOR => DS}
import com.abhioncbr.daflow.commons.CommonSpec
import com.abhioncbr.daflow.commons.Fixture

class FileUtilSpec extends CommonSpec {

  "getFilePathString" should "return only pathPrefix as path string" in {
    val expectedPath: String = Fixture.pathPrefix + DS
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath)
    pathString should not be None
    pathString should be(expectedPath)
  }

  "getFilePathString" should "return pathPrefix & catalogue as path string" in {
    val expectedPath: String = Fixture.pathPrefix + DS + Fixture.catalogueStaticInfixPattern1 + DS
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath1)
    pathString should not be None
    pathString should be(expectedPath)
  }

  "getFilePathString" should "return pathPrefix, catalogue & feed as path string" in {
    val expectedPath
      : String = Fixture.pathPrefix + DS + Fixture.catalogueStaticInfixPattern1 + DS + Fixture.feedStaticInfixParam + DS
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath2)
    pathString should not be None
    pathString should be(expectedPath)
  }

  "getFilePathString" should "return pathPrefix, catalogue, feed and fileName as path string" in {
    val expectedPath: String = Fixture.pathPrefix + DS + Fixture.catalogueStaticInfixPattern1 +
      DS + Fixture.feedStaticInfixParam + DS + Fixture.fileNamePrefix1 + "." + Fixture.fileNameSuffix1
    val pathString: String = FileUtil.getFilePathString(Fixture.dataPath3)
    pathString should not be None
    pathString should be(expectedPath)
  }

}
