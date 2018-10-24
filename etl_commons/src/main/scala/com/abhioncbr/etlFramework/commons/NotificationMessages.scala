package com.abhioncbr.etlFramework.commons

object NotificationMessages {
  val fileNotExist: String => String = (filePath: String) => { s"Provided file path '$filePath' doesn't exist."}
}
