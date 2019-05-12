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

import com.abhioncbr.daflow.commons.transform.TransformConf

class ParseTransformSpec extends XmlJobConfBase {

  "ParseTransform" should "return TransformConf object with array of TransformStepsConf" in {
    val xmlContent = """<transform><step order="23">
                            <rule type="FILTER" group='feed1'><condition>{col1} like 'my%'</condition></rule>
                            <rule type='MERGE' group='feed2' mergeGroup='11,12'/>
                          </step></transform>"""

    val transformConfObject: TransformConf = ParseTransform.fromXML(node(xmlContent))
    transformConfObject should not equal None
    transformConfObject.validateTransformedData should be (false)
    transformConfObject.transformSteps.size should be (1)
    transformConfObject.transformSteps.head.order should be (23)
  }
}
