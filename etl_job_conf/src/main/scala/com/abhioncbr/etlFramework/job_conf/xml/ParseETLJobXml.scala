package com.abhioncbr.etlFramework.job_conf.xml

import java.io._

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.HADOOP_CONF
import com.abhioncbr.etlFramework.commons.extract.{Extract, ExtractionType, QueryParam, QueryParamTypeEnum}
import com.abhioncbr.etlFramework.commons.{Context, ProcessFrequencyEnum}
import com.abhioncbr.etlFramework.commons.job.{ETLJob, FieldMapping, JobStaticParam}
import com.abhioncbr.etlFramework.commons.load.{Load, LoadType, PartitionColumn, PartitionColumnTypeEnum, PartitioningData}
import com.abhioncbr.etlFramework.commons.transform.{AddColumnRule, DummyRule, MergeRule, NilRule, PartitionRule, SchemaTransformationRule, SimpleFunctionRule, Transform, TransformationRule, TransformationStep}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.immutable.ListMap
import scala.util.Try

object JobStaticParam {
  def fromXML(node: scala.xml.NodeSeq): JobStaticParam = {
    val frequency  = (node \ "frequency").text
    new JobStaticParam(processFrequency = ProcessFrequencyEnum.getProcessFrequencyEnum(frequency),
                       feedName = (node \ "feed_name").text, publishStats = (node \ "publish_stats").text.toBoolean)
  }
}

object Extract {
  def fromXML(node: scala.xml.NodeSeq): Extract = {
    val extractionType = ExtractionType.getValueType((node \ "type").text)
    var extract: Extract = null
    if(extractionType.equals(ExtractionType.JSON)) {
      extract = new Extract(extractionType,
        fileInitialPath = String.format((node \ "file_initial_path").text),
        fileNamePattern = (node \ "file_name_pattern").text,
        formatFileName = (node \ "format_file_name").text.toBoolean, filePrefix = (node \ "file_prefix").text, dbPropertyFile= "", queryFilePath = "", queryParams= null, validateExtractedData = (node \ "validate_extracted_data").text.toBoolean)
    } else if(extractionType.equals(ExtractionType.JDBC)) {
      extract = new Extract(extractionType, fileInitialPath = "", fileNamePattern = "", formatFileName = (node \ "format_file_name").text.toBoolean,
        filePrefix = "",
        dbPropertyFile = String.format((node \ "db_property_file_path").text),
        queryFilePath = (node \ "sql_query_file_path").text,
        queryParams = List[QueryParam]((node \ "query_params" \ "param").toList map { s => QueryParam.fromXML(s) }: _*), validateExtractedData = (node \ "validate_extracted_data").text.toBoolean)
    }
    extract
  }
}

object QueryParam {
  def fromXML(node: scala.xml.NodeSeq): QueryParam = {
    val order = (node  \ "@order").text.toInt
    val paramName = (node \ "@name").text
    val paramValue = (node \ "@value").text
    val paramDefaultValue = (node \ "@default_value").text
    new QueryParam(order, paramName, QueryParamTypeEnum.getValueType(paramValue), paramDefaultValue)
  }
}

object Transform {
  def fromXML(node: scala.xml.NodeSeq): Transform = {
    val rules = List[DummyRule]((node \ "rule").toList map { s => Rule.fromXML(s) }: _*).flatten(dummy => Rule.getRules(dummy))
    new Transform(transformationSteps = getTransformationStep(rules), validateTransformedData = (node \ "validate_transformed_data").text.toBoolean)
  }

  def getTransformationStep(rules: List[TransformationRule]) : List[TransformationStep] ={
    val orderedRule = rules.map(rule => (rule.getOrder,rule)).groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    val transformationStepsMap = orderedRule.keys.zip(orderedRule.values.map(list => list.map(rule => (rule.getGroup, rule)).toMap))
    val transformationSteps: List[TransformationStep] = ListMap(transformationStepsMap.toSeq.sortBy(_._1):_*).
      map(entry => new TransformationStep(entry._1,entry._2)).toList


    transformationSteps
  }
}

object FieldMapping {
  def fromXML(node: scala.xml.NodeSeq): FieldMapping = {
    new FieldMapping(sourceFieldName = (node \ "@source_name").text, targetFieldName = (node \ "@target_name").text)
  }
}

object Rule {
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
    val order = (node \ "@order").text.toInt
    val ruleType  = (node \ "@type").text
    val group = (node \ "@group").text.toInt
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
                          val subRules: List[DummyRule] = List[DummyRule]((node \ "rule").toList map { s => Rule.fromXML(s) }: _*)
                          DummyRule(new PartitionRule(order, scope, condition, group), subRules)

      case "SCHEMA_TRANSFORMATION" => val failedFieldLimit = (node \ "@failed_field_limit").text.toInt
                                      val failedRowLimit = (node \ "@failed_row_limit").text.toInt
                                      val mappings = List[FieldMapping]((node \ "field_mapping").toList map { s => FieldMapping.fromXML(s) }: _*)
                                      DummyRule(new SchemaTransformationRule(order, condition, group, mappings, failedFieldLimit, failedRowLimit), null)

    }
    rule
  }
}

object Load {
  def fromXML(node: scala.xml.NodeSeq): Load = {
    new Load(subTask = (node \ "task").text, loadType = LoadType.getValueType((node \ "type").text), dbName = (node \ "db_name").text,
      tableName = (node \ "table_name").text, datasetName= (node \ "dataset").text, feedName= (node \ "feed_name").text, fileInitialPath= (node \ "file_initial_path").text,
      fileType = (node \ "file_type").text, partData = PartitioningData.fromXML(node \ "partition_data"))
  }
}

object PartitioningData {
  def fromXML(node: scala.xml.NodeSeq): PartitioningData = {
    val coalesce = (node \ "coalesce_partition").text.toBoolean
    val overwrite = (node \ "overwrite_partition").text.toBoolean
    val coalesceCount = (node \ "coalesce_partition_count").text.toInt
    val partitionColumns = List[PartitionColumn]((node \ "partition_columns" \ "column").toList map { s => PartitionColumn.fromXML(s) }: _*)
    val partitionFileInitialPath = (node \ "partition_file_initial_path").text
    new PartitioningData(coalesce = coalesce, overwrite = overwrite, coalesceCount = coalesceCount, partitionColumns = partitionColumns, partitionFileInitialPath = partitionFileInitialPath)
  }
}

object PartitionColumn {
  def fromXML(node: scala.xml.NodeSeq): PartitionColumn = {
    val name = (node  \ "@name").text
    val value = (node \ "@value").text
    new PartitionColumn(name, PartitionColumnTypeEnum.getValueType(value))
  }
}

object ETLJob{
  def fromXML(node: scala.xml.NodeSeq): ETLJob = {
      new ETLJob(JobStaticParam.fromXML(node \ "job_static_param"),
        Extract.fromXML(node \ "extract"),
        Transform.fromXML(node \ "transform"),
        Load.fromXML(node \ "load")
    )
  }
}

class ParseETLJobXml {

  def parseXml(path: String, loadFromHdfs: Boolean): Either[String, String] = {
    try {
      var reader: BufferedReader = null

      if(loadFromHdfs) {
        val fs = FileSystem.get(Context.getContextualObject[Configuration](HADOOP_CONF))
        reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      } else {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
      }

      val lines = Stream.continually(reader.readLine()).takeWhile(_ != null).toArray[String].mkString
      reader.close()
      Left(lines)
    } catch {
      case e: FileNotFoundException => Right("Not able to load job xml file. Provided path: " + path)
      case ex: Exception => Right("Exception while parsing job xml file. Please validate xml.")
    }
  }

  def parseNode(node: scala.xml.Node): Either[(JobStaticParam, Extract, Transform, Load), String] ={
    val trimmedNode = scala.xml.Utility.trim(node)
    val etlJob = trimmedNode match {
      case <etl_job>{ children @ _* }</etl_job> => ETLJob.fromXML(trimmedNode)
      case _ => return Right(s"Unknown entity found instead of '<etl_job>'")
    }
    Left((etlJob.jobStaticParam, etlJob.extract, etlJob.transform, etlJob.load))
  }
}

