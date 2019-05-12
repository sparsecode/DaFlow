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

import com.abhioncbr.etlFramework.commons.load.PartitioningDataConf

class ParsePartitioningRuleSpec extends XmlJobConfBase {

  "ParsePartitioningData" should "return PartitioningData object with all variable having values" in {
    val xmlContent = """<partitionData coalescePartition="true" overwritePartition="true" coalescePartitionCount="10">
                      <partitionColumns>
                         <column name="Date" value="date"/>
                      </partitionColumns>
                    </partitionData>""".stripMargin
    val partitioningDataObject: PartitioningDataConf = ParsePartitioningData.fromXML(node(xmlContent))
    partitioningDataObject should not equal None
    partitioningDataObject.coalesce should be (true)
    partitioningDataObject.overwrite should be (true)
    partitioningDataObject.coalesceCount should be (10)
    partitioningDataObject.partitionColumns should not be None
    partitioningDataObject.partitionColumns.length should be (1)
    partitioningDataObject.partitionColumns.head.paramName should be ("Date")
    partitioningDataObject.partitionColumns.head.paramValue should be ("date")
  }

  "ParsePartitioningData" should "return PartitioningData object with only provided variables" in {
    val xmlContent = """<partitionData overwritePartition="true">
                      <partitionColumns>
                         <column name="Date" value="date"/>
                      </partitionColumns>
                    </partitionData>""".stripMargin
    val partitioningDataObject: PartitioningDataConf = ParsePartitioningData.fromXML(node(xmlContent))
    partitioningDataObject should not equal None
    partitioningDataObject.coalesce should be (false)
    partitioningDataObject.overwrite should be (true)
    partitioningDataObject.coalesceCount should be (-1)
    partitioningDataObject.partitionColumns should not be None
    partitioningDataObject.partitionColumns.length should be (1)
    partitioningDataObject.partitionColumns.head.paramName should be ("Date")
    partitioningDataObject.partitionColumns.head.paramValue should be ("date")
  }

}
