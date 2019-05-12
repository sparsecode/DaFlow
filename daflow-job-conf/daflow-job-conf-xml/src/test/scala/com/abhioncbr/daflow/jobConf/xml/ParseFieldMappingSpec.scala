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

import com.abhioncbr.daflow.commons.common.FieldMappingConf

class ParseFieldMappingSpec extends XmlJobConfBase {

  "ParseFieldMapping-fromXML" should "return FieldMapping object" in {
    val xmlContent: String = """<fieldMapping sourceName="source" targetName="target"/>"""
    val filedMappingObject: FieldMappingConf = ParseFieldMapping.fromXML(node(xmlContent))
    filedMappingObject should not equal None
    filedMappingObject.sourceFieldName should be ("source")
    filedMappingObject.targetFieldName should be ("target")
  }


  "ParseFieldMappings-fromXML" should "return FieldMapping object" in {
    val xmlContent: String =
      """<node>
        |<fieldMapping sourceName="source1" targetName="target1"/>
        |<fieldMapping sourceName="source2" targetName="target2"/>
        |<fieldMapping sourceName="source3" targetName="target3"/>
        |</node>""".stripMargin
    val filedMappingArrayObject: List[FieldMappingConf] = ParseFieldMappings.fromXML(node(xmlContent))
    filedMappingArrayObject should not equal None
    filedMappingArrayObject.length should be (3)
    filedMappingArrayObject.head.sourceFieldName should be ("source1")
    filedMappingArrayObject.head.targetFieldName should be ("target1")
  }
}
