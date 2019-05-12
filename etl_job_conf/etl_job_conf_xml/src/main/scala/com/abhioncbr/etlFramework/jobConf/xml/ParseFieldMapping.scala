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

package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.FieldMappingConf

object ParseFieldMappings {
  def fromXML(node: scala.xml.NodeSeq): List[FieldMappingConf] = {
    List[FieldMappingConf]((node \ "fieldMapping").toList map { s => ParseFieldMapping.fromXML(s) }: _*)
  }
}

object ParseFieldMapping {
  def fromXML(node: scala.xml.NodeSeq): FieldMappingConf = {
    FieldMappingConf(sourceFieldName = (node \ "@sourceName").text,
      targetFieldName = (node \ "@targetName").text)
  }
}
