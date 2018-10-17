package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.FieldMapping
import com.abhioncbr.etlFramework.commons.load.PartitionColumnTypeEnum
import com.abhioncbr.etlFramework.commons.transform._

import scala.util.Try

object ParseRule {
  def getRules(dummy: DummyRule): List[TransformationRule] = {
    if(dummy.subRules != null) dummy.rule +: dummy.subRules.flatten(subDummy => getRules(subDummy))
    else  List(dummy.rule)
  }

  def getMergerGroup(mergeGroup: String): Either[(Int,Int), String] = {
    if(!mergeGroup.isEmpty){
      val temp = mergeGroup.split(",").map(group => Try(group.trim.toInt)).partition(result => result.isSuccess )._1
      if(temp.length ==2 ) return Left((temp.head.get, temp.tail.head.get))
    }
    Right("not proper merge group input")
  }

  def fromXML(node: scala.xml.NodeSeq): DummyRule = {
    val order = ParseUtil.parseInt((node \ "@order").text)
    val ruleType  = (node \ "@type").text
    val group = ParseUtil.parseInt((node \ "@group").text)
    val condition = (node \ "condition").text

    val rule = ruleType match {
      case "FILTER" => DummyRule(new SimpleFunctionRule(ruleType, order, condition, group), null)

      case "SELECT" => DummyRule(new SimpleFunctionRule(ruleType, order, condition, group), null)

      case "DROP" =>  DummyRule(new SimpleFunctionRule(ruleType, order, condition, group), null)

      case "EXPLODE" => DummyRule(new SimpleFunctionRule(ruleType, order, condition, group), null)

      case "NIL" => DummyRule(new NilRule(order, group), null)

      case "ADD_COLUMN" => val columnName = (node \ "@column_name").text
        val columnValueType = (node \ "@column_value").text
        DummyRule(new AddColumnRule(order, group, columnName, PartitionColumnTypeEnum.getValueType(columnValueType)), null)

      case "MERGE" => val mergeGroup = (node \ "@merge_group").text
        DummyRule(new MergeRule(order, condition, getMergerGroup(mergeGroup).left.get, group), null)

      case "PARTITION" => val scope = (node \ "@scope").text
        val subRules: List[DummyRule] = List[DummyRule]((node \ "rule").toList map { s => ParseRule.fromXML(s) }: _*)
        DummyRule(new PartitionRule(order, scope, condition, group), subRules)

      case "SCHEMA_TRANSFORMATION" => val failedFieldLimit = ParseUtil.parseInt((node \ "@failed_field_limit").text)
        val failedRowLimit = ParseUtil.parseInt((node \ "@failed_row_limit").text)
        val mappings = List[FieldMapping]((node \ "field_mapping").toList map { s => ParseFieldMapping.fromXML(s) }: _*)
        DummyRule(new SchemaTransformationRule(order, condition, group, mappings, failedFieldLimit, failedRowLimit), null)

    }
    rule
  }
}
