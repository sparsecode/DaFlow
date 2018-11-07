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

package com.abhioncbr.etlFramework.commons

object NotificationMessages {
  val fileDoesNotExist: String => String =
    (filePath: String) => { s"Provided file path '$filePath' doesn't exist." }

  val jobXmlFileDoesNotExist: String => String =
    (filePath: String) => { s"Not able to load job xml file. Provided path: '$filePath'" }

  val exceptionMessage: Exception => String =
    (exception: Exception) => { s"Exception message: ${exception.getMessage}" }

  val unknownXMLEntity: String = "Unknown entity found instead of '<etlJob>'"
  val exceptionWhileParsing: String = "Exception while parsing job xml file. Please validate xml."

  // extract
  val extractNotSupported: String => String =
    (extractType: String) => { s"extracting data from $extractType is not supported right now" }
}
