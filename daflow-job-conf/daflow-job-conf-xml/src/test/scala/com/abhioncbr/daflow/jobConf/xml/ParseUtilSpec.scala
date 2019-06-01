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

import com.abhioncbr.daflow.commons.NotificationMessages
import com.abhioncbr.daflow.commons.common.DataPath

class ParseUtilSpec extends XmlJobConfBase {

  "parseBoolean" should "return false boolean object when blank string or null is passed as an argument" in {
    val booleanObject: Boolean = ParseUtil.parseBoolean(text = "")
    booleanObject should not equal None
    booleanObject should be (false)

    val text: Option[String] = None
    val anotherBooleanObject: Boolean = ParseUtil.parseBoolean(text = text.orNull)
    anotherBooleanObject should not equal None
    anotherBooleanObject should be (false)
  }

  "parseBoolean" should "return false boolean object when 'false' or some other value is passed as a string argument" in {
    val booleanObject: Boolean = ParseUtil.parseBoolean(text = "false")
    booleanObject should not equal None
    booleanObject should be (false)

    val anotherBooleanObject: Boolean = ParseUtil.parseBoolean(text = "aa")
    anotherBooleanObject should not equal None
    anotherBooleanObject should be (false)
  }

  "parseBoolean" should "return true boolean object when 'true' or 'True' or 'TRUE' is passed as a string argument" in {
    val booleanObject: Boolean = ParseUtil.parseBoolean(text = "true")
    booleanObject should not equal None
    booleanObject should be (true)

    val anotherBooleanObject: Boolean = ParseUtil.parseBoolean(text = "True")
    anotherBooleanObject should not equal None
    anotherBooleanObject should be (true)

    val oneMoreBooleanObject: Boolean = ParseUtil.parseBoolean(text = "TRUE")
    oneMoreBooleanObject should not equal None
    oneMoreBooleanObject should be (true)
  }

  "parseInt" should "return [-1] Int object when blank string or null or any other non-int is passed as an argument" in {
    val intObject: Int = ParseUtil.parseInt(text = "")
    intObject should not equal None
    intObject should be (-1)

    val text: Option[String] = None
    val anotherIntObject: Int = ParseUtil.parseInt(text = text.orNull)
    anotherIntObject should not equal None
    anotherIntObject should be (-1)

    val oneMoreIntObject: Int = ParseUtil.parseInt(text = "aa")
    oneMoreIntObject should not equal None
    oneMoreIntObject should be (-1)

    val yetAnotherIntObject: Int = ParseUtil.parseInt(text = "-1")
    yetAnotherIntObject should not equal None
    yetAnotherIntObject should be (-1)
  }

  "parseInt" should "return Int object with specified value when value is passed as an argument" in {
    val intObject: Int = ParseUtil.parseInt(text = "20")
    intObject should not equal None
    intObject should be (20)

    val oneMoreIntObject: Int = ParseUtil.parseInt(text = "-20")
    oneMoreIntObject should not equal None
    oneMoreIntObject should be (-20)
  }

  "parseFilePathString" should "should return Either[Right] object when blank string is passed as an argument" in {
    val filePathObject: Either[DataPath, String] = ParseUtil.parseFilePathString(text = "")
    filePathObject should not equal None
    filePathObject.isRight should be (true)
    filePathObject.isLeft should be (false)
    filePathObject.right.get should be ("Can not create a Path from an empty string")

    val text: Option[String] = None
    val anotherFilePathObject: Either[DataPath, String] = ParseUtil.parseFilePathString(text = text.orNull)
    anotherFilePathObject should not equal None
    anotherFilePathObject.isRight should be (true)
    anotherFilePathObject.isLeft should be (false)
    anotherFilePathObject.right.get should be ("Can not create a Path from a null string")
  }

  "parseFilePathString" should "should return Either[Left] object when value is passed as an argument" in {
    val path = s"$daflowExamplesDemoSampleDataPath/json_data.json"
    val filePathObject: Either[DataPath, String] = ParseUtil.parseFilePathString(path)
    filePathObject should not equal None
    filePathObject.isRight should be (false)
    filePathObject.isLeft should be (true)
    filePathObject.left.get should not equal None
    filePathObject.left.get.pathPrefix should contain (daflowExamplesDemoSampleDataPath)
  }

    /*
    "parseFilePathString" should "should return Either[Right] object when value is passed as an argument" in {
    val path = s"$daflowExamplesDemoSampleDataPath/json_data.json"
    val filePathObject: Either[DataPath, String] = ParseUtil.parseFilePathString(path)
    filePathObject should not equal None
    filePathObject.isRight should be (true)
    filePathObject.isLeft should be (false)
    filePathObject.right.get should not equal None
    filePathObject.right.get should equal (NotificationMessages.fileDoesNotExist(path))
    }
    */
}
