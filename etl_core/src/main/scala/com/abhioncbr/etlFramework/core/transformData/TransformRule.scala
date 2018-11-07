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

package com.abhioncbr.etlFramework.core.transformData

import com.abhioncbr.etlFramework.commons.{Context, ExecutionResult}
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.common.FieldMappingConf
import com.abhioncbr.etlFramework.commons.load.LoadFeedConf
import com.abhioncbr.etlFramework.sqlParser.Clause
import com.abhioncbr.etlFramework.sqlParser.SQLParser
import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import scala.util.Try

trait TransformRule {
  def getGroup: String
  def condition(f: String => DataFrame): Boolean
  def execute(f: String => DataFrame): Either[Array[ExecutionResult], String]
}

class NilRule(group: String) extends TransformRule {
  override def getGroup: String = group
  override def condition(f: String => DataFrame): Boolean = true

  override def execute(f: String => DataFrame): Either[Array[ExecutionResult], String] =
    Left(Array(ExecutionResult(group, f.apply(group))))

  override def toString: String = { s"ruleName = nil, group = $group" }
}

class MergeRule(ruleCondition: String, mergeGroup: (String, String), group: String) extends TransformRule {
  def getMergeGroup: (String, String) = mergeGroup
  override def getGroup: String = group

  override def toString: String = { s"ruleName = merge, merge_group = '$mergeGroup', " +
    s"group = $group, ruleCondition = '$ruleCondition'"}

  def condition(f: String => DataFrame): Boolean = {
    Try(f.apply(mergeGroup._1)).isSuccess &&  Try(f.apply(mergeGroup._2)).isSuccess
  }

  def execute(f: String => DataFrame): Either[Array[ExecutionResult], String] = {
    val df1: DataFrame = f.apply(mergeGroup._1)
    val df2: DataFrame = f.apply(mergeGroup._2)
    if(df1.count > 0 && df2.count <= 0) Left(Array(ExecutionResult(group, df1)))
    if(df1.count <= 0 && df2.count > 0) Left(Array(ExecutionResult(group, df2)))

    if(df1.schema == df2.schema) { Left(Array(ExecutionResult(group, df1.union(df2)))) }
    else { Right(s"DataFrames of group $mergeGroup, schema is not equal") }
  }
}

abstract class AbstractTransformationRule(group: String, condition: String) extends TransformRule {
  private val logger = Logger(this.getClass)
  override def getGroup: String = group
  override def toString: String = { s"group = $group, ruleCondition = '$condition'" }
  override def condition(f: String => DataFrame): Boolean = condition(f.apply(group))
  override def execute(f: String => DataFrame): Either[Array[ExecutionResult], String] = execute(f.apply(group))

  def condition(inputDataFrame: DataFrame): Boolean
  def execute(inputDataFrame: DataFrame): Either[Array[ExecutionResult], String]

  def analyzeWhereCondition(inputDataFrame: DataFrame): (Boolean, Option[Seq[Clause]]) = {
    try {
      val whereClauses = new SQLParser().getWhereClause(condition)

      val output: Boolean = whereClauses.get.map(clause => {
        !TransformUtil.hasColumn(inputDataFrame, clause.getFields("field").toString)
      }).reduce(_ && _)

      (output, Some(whereClauses.get))
    } catch {
      case ex: Exception => logger.error(ex.getMessage)
        (false, None)
    }
  }

  def analyzeSelectCondition(inputDataFrame: DataFrame): (Boolean, Option[Seq[String]]) = {
    try {
      val selectClauses = new SQLParser().getSelectClause(condition)
      val output: Boolean = selectClauses.get.map(str => TransformUtil.hasColumn(inputDataFrame, str)).reduce(_ && _)
      (output, Some(selectClauses.get))
    } catch {
      case ex: Exception => logger.error(ex.getMessage)
        (false, None)
    }
  }
}

// Added on Sept-17, for adding column in to dataframe(currently, column values is only of type partition column)
// TODO/FIXME: Need to enhance in-future to support column values based on some logic.
class AddColumnRule(group: String, columnName: String, columnValueType: String) extends TransformRule {
  override def getGroup: String = group

  override def toString: String = { s"ruleName = addColumn, group = $group"}

  override def condition(f: String => DataFrame): Boolean = true

  override def execute(f: String => DataFrame): Either[Array[ExecutionResult], String] = {
    import org.apache.spark.sql.functions.lit
    val value: Column = lit(literal = "")
    val inputDataFrame: DataFrame = f.apply(group)
    Left(Array(ExecutionResult(group, inputDataFrame.withColumn(columnName, value))))
  }
}

class SimpleFunctionRule(functionType: String, ruleCondition: String, group: String)
  extends AbstractTransformationRule(group, ruleCondition) {

  override def toString: String = { s"ruleName = SimpleFunctionRule, functionType= $functionType, " + super.toString}

  val whereClause: scala.collection.mutable.Seq[Clause] = scala.collection.mutable.Seq[Clause]()
  val selectColumns: scala.collection.mutable.Seq[String] = scala.collection.mutable.Seq[String]()

  def condition(inputDataFrame: DataFrame): Boolean = {
    functionType match {
      case "FILTER" => val output = analyzeWhereCondition(inputDataFrame)
        whereClause :+ output._2.get
        output._1

      case "SELECT" => val output = analyzeSelectCondition(inputDataFrame)
        selectColumns :+ output._2.get
        output._1

      case "DROP" => val output = analyzeSelectCondition(inputDataFrame)
        selectColumns :+ output._2.get
        output._1

      case "EXPLODE" => val output = analyzeSelectCondition(inputDataFrame)
        selectColumns :+ output._2.get
        output._1
    }
  }

  def execute(inputDataFrame: DataFrame): Either[Array[ExecutionResult], String] = {
    functionType match {
      case "FILTER" => Left(filter(inputDataFrame))
      case "SELECT" => Left(select(inputDataFrame))
      case "DROP" => Left(drop(inputDataFrame))
      case "EXPLODE" => Left(explode(inputDataFrame))
    }
  }

  def filter(inputDataFrame: DataFrame): Array[ExecutionResult] = {
    Array(ExecutionResult(group, inputDataFrame.filter(ruleCondition)))
  }

  def select(inputDataFrame: DataFrame): Array[ExecutionResult] = {
    val selectColumns = ruleCondition.split(",")
    // added condition for supporting column like 'http-agent'
    val selectCondition = selectColumns.map(str => if (str.contains(".")) str.trim else s"`${str.trim}`").mkString(", ")

    val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    inputDataFrame.createOrReplaceTempView(viewName = "temp")
    Array(ExecutionResult(group, sqlContext.sql(sqlText = s"select $selectCondition from temp")))
  }

  def drop(inputDataFrame: DataFrame): Array[ExecutionResult] = {
    val outputDF = inputDataFrame.drop(selectColumns.map(col => s""" "$col" """).mkString(" , "))
    Array(ExecutionResult(group, outputDF))
  }

  def explode(inputDataFrame: DataFrame): Array[ExecutionResult] = {
    var outputDF = inputDataFrame

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    import sqlContext.implicits._
    selectColumns.foreach(str => outputDF = outputDF.select(org.apache.spark.sql.functions.explode($"$str").alias(s"$str")))
    Array(ExecutionResult(group, outputDF))
  }
}

class PartitionRule(scope: String, ruleCondition: String, group: String)
  extends AbstractTransformationRule(group, ruleCondition) {
  private var condition: String = _
  private var notCondition: String = _

  override def toString: String = { s"ruleName = partition, scope = $scope, " + super.toString}

  def condition(inputDataFrame: DataFrame): Boolean = {
    val output = analyzeWhereCondition(inputDataFrame)
    setCondition(output._2.get)
    output._1
  }

  def execute(inputDataFrame: DataFrame): Either[Array[ExecutionResult], String] = {
    val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    inputDataFrame.createGlobalTempView(viewName = "temp")

    val dataFrame = sqlContext.sql(sqlText = s"select * from temp where $condition")
    val negateDataFrame = sqlContext.sql(sqlText = s"select * from temp where $notCondition")
    Left(Array(ExecutionResult(group, dataFrame), ExecutionResult(group, negateDataFrame)))
  }

  private def setCondition(conditions: Seq[Clause]): Unit = {
    conditions.foreach {
      case notNullClause: com.abhioncbr.etlFramework.sqlParser.NotNull =>
        condition = s"${notNullClause.getFields("field")} IS NOT NULL"
        notCondition = s"${notNullClause.getFields("field")} IS NULL"

      case nullClause: com.abhioncbr.etlFramework.sqlParser.Null =>
        condition = s"${nullClause.getFields("field")} IS NULL"
        notCondition = s"${nullClause.getFields("field")} IS NOT NULL"
    }
  }
}

// TODO/FIXME: rule condition more dynamic
class SchemaTransformationRule(ruleCondition: String, group: String, fieldMapping: List[FieldMappingConf],
  failedFieldLimit: Int, failedRowLimit: Int) extends AbstractTransformationRule(group, ruleCondition) {

  private var tableSchema: StructType = null
  private val logger = Logger(this.getClass)

  override def toString: String = {s"ruleName = schema_transformation, ruleCondition = $ruleCondition, " +
    s"failedFieldLimit = $failedFieldLimit, failedRowLimit = $failedRowLimit, " +
    s"fieldMapping = $fieldMapping, " + super.toString }

  def condition(inputDataFrame: DataFrame): Boolean = {
    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val tableName = Context.getContextualObject[LoadFeedConf](LOAD_CONF).attributesMap("tableName")
    val databaseName = Context.getContextualObject[LoadFeedConf](LOAD_CONF).attributesMap("databaseName")
    val partitionColumns: List[String] = Context.getContextualObject[LoadFeedConf](LOAD_CONF).partitioningData.get.partitionColumns.map(column => column.paramName)

    val temp = TransformUtil.tableMetadata(tableName, databaseName, sqlContext, partitionColumns)

    if (temp._1.isDefined) {
      tableSchema = temp._1.get
      // adding schema in to context for re-use in validation step.
      Context.addContextualObject[(Option[StructType], Option[StructType])](SCHEMA, temp)

    } else {
      logger.error(s"Table '$tableName' doesn't exist in database '$databaseName'")
    }
    temp._1.isDefined
  }

  def execute(inputDataFrame: DataFrame): Either[Array[ExecutionResult], String] = {
    val tableColumns: List[(Int, String, DataType)] = TransformUtil.getColumnsInfo(tableSchema,
      fieldMapping.map(mapping => (mapping.targetFieldName,mapping.sourceFieldName)).toMap)

    //TODO: pre append rule condition
    val preAppend = ruleCondition
    val temp: RDD[(Int, Row)] = inputDataFrame.rdd.map(row => TransformUtil.flattenStructType(row, preAppend)).map(row => {
      var failCount = 0
      val array:Array[Any] = tableColumns.map(ele => TransformUtil.selectColumn(row, ele._3, ele._2)).toArray.map(
        either => if(either.isLeft)
          either.left.get
        else {
          failCount =  failCount+1
          logger.error(either.right.get)
          null
        }
      )
      (failCount, Row.fromSeq(array))
    })

    val TRUE: Boolean = true
    val FALSE: Boolean = false

    val validRDD: RDD[(Int, Row)] = filterRdd(TRUE, failedFieldLimit, temp)
    val invalidRDD: RDD[(Int, Row)] = filterRdd(FALSE, failedFieldLimit, temp)
    val validCount: Int = validRDD.count.toInt
    val invalidCount: Int = invalidRDD.count.toInt

    //.filter(tuple => tuple._1 <= failedFieldLimit)
    //partition(tuple => tuple._1 <= failedFieldLimit)

    logger.info(s"failed schema transformation rows count: $invalidCount")
    logger.info(s"passed schema transformation rows count: $validCount")

    if(invalidCount > failedRowLimit)
      return Right(s"Aborting job because number of row failed($invalidCount) " +
        s"in schema transformation is greater then threshold($failedRowLimit)")

    val transformedRDD = validRDD.map(tuple => tuple._2)
    // val sparkContext: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
    // val transformedRDD = sparkContext.makeRDD(transformedSequence)

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val outputDF = sqlContext.createDataFrame(transformedRDD, schema = tableSchema)

    Left(Array(ExecutionResult(group, outputDF, Some(Map("validCount" -> validCount, "invalidCount" -> invalidCount)))))
  }

  def filterRdd(valid: Boolean, failedFieldLimit: Int, temp: RDD[(Int, Row)]): RDD[(Int, Row)] = {
    if (valid) { temp.filter(_._1 <= failedFieldLimit) } else { temp.filter(_._1 > failedFieldLimit) }
  }
}