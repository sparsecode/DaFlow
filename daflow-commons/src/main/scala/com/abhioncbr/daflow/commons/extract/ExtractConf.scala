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

package com.abhioncbr.daflow.commons.extract

import com.abhioncbr.daflow.commons.common.DataPath
import com.abhioncbr.daflow.commons.common.QueryConf

case class ExtractConf(feeds: Array[ExtractFeedConf])
case class ExtractFeedConf(extractFeedName: String, extractionType: ExtractionType.valueType,
  extractionAttributesMap: Map[String, String], dataPath: Option[DataPath], query: Option[QueryConf],
  validateExtractedData: Boolean)
