package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.NotificationMessages
import com.abhioncbr.etlFramework.commons.common.file.FilePath

class ParseUtilSpec extends XmlJobConfBase {

  "parseBoolean" should "return false boolean object when blank string or null is passed as an argument" in {
    val booleanObject: Boolean = ParseUtil.parseBoolean( text = "")
    booleanObject should not equal null
    booleanObject should be (false)

    val anotherBooleanObject: Boolean = ParseUtil.parseBoolean( text = null)
    anotherBooleanObject should not equal null
    anotherBooleanObject should be (false)
  }

  "parseBoolean" should "return false boolean object when 'false' or some other value is passed as a string argument" in {
    val booleanObject: Boolean = ParseUtil.parseBoolean( text = "false")
    booleanObject should not equal null
    booleanObject should be (false)

    val anotherBooleanObject: Boolean = ParseUtil.parseBoolean( text = "aa")
    anotherBooleanObject should not equal null
    anotherBooleanObject should be (false)
  }

  "parseBoolean" should "return true boolean object when 'true' or 'True' or 'TRUE' is passed as a string argument" in {
    val booleanObject: Boolean = ParseUtil.parseBoolean( text = "true")
    booleanObject should not equal null
    booleanObject should be (true)

    val anotherBooleanObject: Boolean = ParseUtil.parseBoolean( text = "True")
    anotherBooleanObject should not equal null
    anotherBooleanObject should be (true)

    val oneMoreBooleanObject: Boolean = ParseUtil.parseBoolean( text = "TRUE")
    oneMoreBooleanObject should not equal null
    oneMoreBooleanObject should be (true)
  }

  "parseInt" should "return [-1] Int object when blank string or null or any other non-int is passed as an argument" in {
    val intObject: Int = ParseUtil.parseInt( text = "")
    intObject should not equal null
    intObject should be (-1)

    val anotherIntObject: Int = ParseUtil.parseInt( text = null)
    anotherIntObject should not equal null
    anotherIntObject should be (-1)

    val oneMoreIntObject: Int = ParseUtil.parseInt( text = "aa")
    oneMoreIntObject should not equal null
    oneMoreIntObject should be (-1)

    val yetAnotherIntObject: Int = ParseUtil.parseInt( text = "-1")
    yetAnotherIntObject should not equal null
    yetAnotherIntObject should be (-1)
  }

  "parseInt" should "return Int object with specified value when value is passed as an argument" in {
    val intObject: Int = ParseUtil.parseInt( text = "20")
    intObject should not equal null
    intObject should be (20)

    val oneMoreIntObject: Int = ParseUtil.parseInt( text = "-20")
    oneMoreIntObject should not equal null
    oneMoreIntObject should be (-20)
  }

  "parseFilePathString" should "should return Either[Right] object when blank string is passed as an argument" in {
    val filePathObject: Either[FilePath, String] = ParseUtil.parseFilePathString("")
    filePathObject should not equal null
    filePathObject.isRight should be (true)
    filePathObject.isLeft should be (false)
    filePathObject.right.get should be ("Can not create a Path from an empty string")

    val anotherFilePathObject: Either[FilePath, String] = ParseUtil.parseFilePathString(null)
    anotherFilePathObject should not equal null
    anotherFilePathObject.isRight should be (true)
    anotherFilePathObject.isLeft should be (false)
    anotherFilePathObject.right.get should be ("Can not create a Path from a null string")
  }

  "parseFilePathString" should "should return Either[Left] object when value is passed as an argument" in {
    val path = s"${System.getProperty("user.dir")}/etl_examples/sample_data/json_data.json"
    val filePathObject: Either[FilePath, String] = ParseUtil.parseFilePathString(path)
    filePathObject should not equal null
    filePathObject.isRight should be (false)
    filePathObject.isLeft should be (true)
    filePathObject.left.get should not equal null
    filePathObject.left.get.pathPrefix should contain (s"${System.getProperty("user.dir")}/etl_examples/sample_data")
  }

  "parseFilePathString" should "should return Either[Right] object when value is passed as an argument" in {
    val path = s"${System.getProperty("user.dir")}/etl_examples/sample_data/json_data.json1"
    val filePathObject: Either[FilePath, String] = ParseUtil.parseFilePathString(path)
    filePathObject should not equal null
    filePathObject.isRight should be (true)
    filePathObject.isLeft should be (false)
    filePathObject.right.get should not equal null
    filePathObject.right.get should equal (NotificationMessages.fileNotExist(path))
  }

}
