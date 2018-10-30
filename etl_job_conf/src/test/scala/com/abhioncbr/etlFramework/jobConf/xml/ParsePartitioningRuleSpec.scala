package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.load.PartitioningData

class ParsePartitioningRuleSpec extends XmlJobConfBase {

  "ParsePartitioningData" should "return PartitioningData object with all variable having values" in {
    val xmlContent = s"""<partitionData coalescePartition="true" overwritePartition="true" coalescePartitionCount="10">
                      <partitionColumns>
                         <column name="Date" value="date"/>
                      </partitionColumns>
                    </partitionData>""".stripMargin
    val partitioningDataObject: PartitioningData = ParsePartitioningData.fromXML(node(xmlContent))
    partitioningDataObject should not equal null
    partitioningDataObject.coalesce should be (true)
    partitioningDataObject.overwrite should be (true)
    partitioningDataObject.coalesceCount should be (10)
    partitioningDataObject.partitionColumns should not be null
    partitioningDataObject.partitionColumns.length should be (1)
    partitioningDataObject.partitionColumns.head.paramName should be ("Date")
    partitioningDataObject.partitionColumns.head.paramValue should be ("date")
  }

  "ParsePartitioningData" should "return PartitioningData object with only provided variables" in {
    val xmlContent = s"""<partitionData overwritePartition="true">
                      <partitionColumns>
                         <column name="Date" value="date"/>
                      </partitionColumns>
                    </partitionData>""".stripMargin
    val partitioningDataObject: PartitioningData = ParsePartitioningData.fromXML(node(xmlContent))
    partitioningDataObject should not equal null
    partitioningDataObject.coalesce should be (false)
    partitioningDataObject.overwrite should be (true)
    partitioningDataObject.coalesceCount should be (-1)
    partitioningDataObject.partitionColumns should not be null
    partitioningDataObject.partitionColumns.length should be (1)
    partitioningDataObject.partitionColumns.head.paramName should be ("Date")
    partitioningDataObject.partitionColumns.head.paramValue should be ("date")
  }

}
