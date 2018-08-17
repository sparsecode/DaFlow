package com.abhioncbr.etlFramework.etl.feed.transformData

import com.abhioncbr.etlFramework.etl.feed.Logger
import com.abhioncbr.etlFramework.etl.feed.common.ContextConstantEnum._
import com.abhioncbr.etlFramework.etl.feed.common.sql_parser.{Clause, SQLParser}
import com.abhioncbr.etlFramework.etl.feed.common.{Context, FieldMapping, Load}
import com.abhioncbr.etlFramework.etl.feed.loadData.hive.PartitionColumnTypeEnum
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, Column}

import scala.util.Try

trait TransformationRule {
  def getOrder: Int
  def getGroup: Int
  def condition(f: Int => DataFrame): Boolean
  def execute(f: Int => DataFrame): Either[Array[(DataFrame,Any,Any)], String]
}

class NilRule(order:Int, group: Int = 0) extends TransformationRule {
  override def getOrder: Int = order
  override def getGroup: Int = group
  override def condition(f: Int => DataFrame): Boolean = true
  override def execute(f: Int => DataFrame): Either[Array[(DataFrame, Any, Any)], String] = Left(Array( (f.apply(group), null, null) ))
  override def toString: String = { s"ruleName = nil, order = $order, group = $group"}
}

class MergeRule(order: Int, ruleCondition: String, mergeGroup: (Int,Int), group: Int) extends TransformationRule {
  def getMergeGroup: (Int, Int) = mergeGroup
  override def getOrder: Int = order
  override def getGroup: Int = group
  override def toString: String = { s"ruleName = merge, merge_group = '$mergeGroup', order = $order, group = $group, ruleCondition = '$ruleCondition'"}

  def condition(f: Int => DataFrame): Boolean = {
    Try(f.apply(mergeGroup._1)).isSuccess &&  Try(f.apply(mergeGroup._2)).isSuccess
  }

  def execute(f: Int => DataFrame): Either[Array[(DataFrame, Any, Any)], String] = {
    val df1 = f.apply(mergeGroup._1)
    val df2 = f.apply(mergeGroup._2)
    if(df1.count > 0 && df2.count <= 0) Left(Array( (df1, null, null) ))
    if(df1.count <= 0 && df2.count > 0) Left(Array( (df2, null, null) ))

    if(df1.schema == df2.schema) Left(Array((df1.unionAll(df2), null, null) ))
    else Right(s"DataFrames of group $mergeGroup, schema is not equal")
  }
}

abstract class abstractTransformationRule(order:Int, group: Int = 0, condition: String) extends TransformationRule{
  override def getOrder: Int = order
  override def getGroup: Int = group
  override def toString: String = { s"order = $order, group = $group, ruleCondition = '$condition'" }
  override def condition(f: Int => DataFrame) = condition(f.apply(group))
  override def execute(f: Int => DataFrame) = execute(f.apply(group))

  def condition(inputDataFrame: DataFrame): Boolean
  def execute(inputDataFrame: DataFrame): Either[Array[ (DataFrame, Any, Any) ], String]

  def analyzeWhereCondition(inputDataFrame: DataFrame): (Boolean, Seq[Clause]) ={
    try{
      val whereClauses = new SQLParser().getWhereClause(condition)

      var output = true
      whereClauses.get.foreach(clause => {
        val field = clause.getFields("field").toString
        if(!TransformUtil.hasColumn(inputDataFrame, field)) output = false
      })
      (output, whereClauses.get)
    } catch {
      case ex: Exception => Logger.log.info(s"analyzeWhereCondition - returning false & null")
        Logger.log.error(ex)
        (false, null)
    }
  }

  def analyzeSelectCondition(inputDataFrame: DataFrame): (Boolean, Seq[String]) ={
    try{
      val selectClauses = new SQLParser().getSelectClause(condition)
      var output = true
      selectClauses.get.foreach(str => {
        if(!TransformUtil.hasColumn(inputDataFrame, str)) output = false
      })
      (output, selectClauses.get)
    } catch {
      case ex: Exception => (false, null)
    }
  }
}

//Added on Sept-17, for adding column in to dataframe(currently, column values is only of type partition column)
//TODO: Need to enhance in-future to support column values based on some logic.
class AddColumnRule(order:Int, group: Int = 0, columnName: String, columnValueType: PartitionColumnTypeEnum.valueType) extends TransformationRule {
  override def getOrder: Int = order
  override def getGroup: Int = group
  override def condition(f: Int => DataFrame): Boolean = true
  override def execute(f: Int => DataFrame): Either[Array[(DataFrame, Any, Any)], String] = {
    import org.apache.spark.sql.functions.lit
    val value:Column = lit(PartitionColumnTypeEnum.getDataValue(columnValueType))

    val inputDataFrame:DataFrame = f.apply(group)

    Left(Array((inputDataFrame.withColumn(columnName,value), null, null) ))
  }
  override def toString: String = { s"ruleName = addColumn, order = $order, group = $group"}
}


class SimpleFunctionRule(functionType:String, order: Int, ruleCondition: String, group: Int)
  extends abstractTransformationRule(order, group, ruleCondition) {
  override def toString: String = { s"ruleName = SimpleFunctionRule, functionType= $functionType, " + super.toString}

  var whereClause: Seq[Clause] = _
  var selectColumns: Seq[String] = _
  def condition(inputDataFrame: DataFrame): Boolean = {
    functionType match {
      case "FILTER" => val output = analyzeWhereCondition(inputDataFrame)
        whereClause = output._2
        output._1
      case "SELECT" => val output = analyzeSelectCondition(inputDataFrame); selectColumns = output._2; output._1
      case "DROP" => val output = analyzeSelectCondition(inputDataFrame); selectColumns = output._2; output._1
      case "EXPLODE" =>  val output = analyzeSelectCondition(inputDataFrame); selectColumns = output._2; output._1
    }
  }

  def execute(inputDataFrame: DataFrame): Either[Array[ (DataFrame, Any, Any) ], String] = {
    functionType match {
      case "FILTER" => Left(filter(inputDataFrame))
      case "SELECT" => Left(select(inputDataFrame))
      case "DROP" => Left(drop(inputDataFrame))
      case "EXPLODE" =>  Left(explode(inputDataFrame))
    }
  }

  def filter(inputDataFrame: DataFrame): Array[ (DataFrame, Any, Any) ] = {
    Array((inputDataFrame.filter(ruleCondition), null, null))
  }

  def select(inputDataFrame: DataFrame): Array[ (DataFrame, Any, Any) ] = {
    val selectColumns = ruleCondition.split(",")
    //added condition for supporting column like 'http-agent'
    val selectCondition = selectColumns.map(str => if(str.contains(".")) str.trim else s"`${str.trim}`").mkString(", ")

    val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    inputDataFrame.registerTempTable("temp")
    Array((sqlContext.sql(s"select $selectCondition from temp"), null, null))
  }

  def drop(inputDataFrame: DataFrame): Array[ (DataFrame, Any, Any) ] = {
    var outputDF = inputDataFrame
    selectColumns.foreach(str => outputDF = outputDF.drop(str))
    Array((outputDF, null, null))
  }

  def explode(inputDataFrame: DataFrame): Array[ (DataFrame, Any, Any) ] = {
    var outputDF = inputDataFrame

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    import sqlContext.implicits._
    selectColumns.foreach(str => outputDF = outputDF.select(org.apache.spark.sql.functions.explode($"$str").alias(s"$str")))
    Array((outputDF, null, null))
  }
}

class PartitionRule(order: Int, scope: String, ruleCondition: String, group: Int)
  extends abstractTransformationRule(order, group, ruleCondition) {
  private var condition: String = _
  private var notCondition: String = _

  override def toString: String = { s"ruleName = partition, scope = $scope, " + super.toString}

  def condition(inputDataFrame: DataFrame): Boolean = {
    val output = analyzeWhereCondition(inputDataFrame)
    setCondition(output._2)
    output._1
  }

  def execute(inputDataFrame: DataFrame): Either[Array[ (DataFrame, Any, Any) ], String] = {
    val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    inputDataFrame.registerTempTable("temp")

    val dataFrame = sqlContext.sql(s"select * from temp where $condition")
    val negateDataFrame = sqlContext.sql(s"select * from temp where $notCondition")
    Left(Array((dataFrame, null, null), (negateDataFrame, null, null)))
  }

  private def setCondition(conditions: Seq[Clause]): Unit = {
    conditions.foreach {
      case notNullClause: com.abhioncbr.etlFramework.etl.feed.common.sql_parser.NotNull =>
        condition = s"${notNullClause.getFields("field")} IS NOT NULL"
        notCondition = s"${notNullClause.getFields("field")} IS NULL"

      case nullClause: com.abhioncbr.etlFramework.etl.feed.common.sql_parser.Null =>
        condition = s"${nullClause.getFields("field")} IS NULL"
        notCondition = s"${nullClause.getFields("field")} IS NOT NULL"
    }
  }
}

//TODO: rule condition more dynamic
class SchemaTransformationRule(order: Int, ruleCondition: String, group: Int, fieldMapping: List[FieldMapping], failedFieldLimit: Int, failedRowLimit: Int)
  extends abstractTransformationRule(order, group, ruleCondition) {
  private var tableSchema:StructType = _

  override def toString: String = {s"ruleName = schema_transformation, ruleCondition = $ruleCondition, " +
    s"failedFieldLimit = $failedFieldLimit, failedRowLimit = $failedRowLimit, fieldMapping = $fieldMapping, " + super.toString }

  def condition(inputDataFrame: DataFrame): Boolean = {
    val hiveContext: HiveContext = Context.getContextualObject[HiveContext](HIVE_CONTEXT)
    val tableName = Context.getContextualObject[Load](LOAD).tableName
    val databaseName = Context.getContextualObject[Load](LOAD).dbName
    val partitionColumns: List[String] = Context.getContextualObject[Load](LOAD).partData.partitionColumns.map(column => column.columnName)

    val temp = TransformUtil.tableMetadata(tableName, databaseName, hiveContext, partitionColumns)

    //adding schema in to context for re-use in validation step.
    Context.addContextualObject[(Option[StructType], Option[StructType])](SCHEMA, temp)

    if(temp._1.isDefined) tableSchema = temp._1.get
    else Logger.log.error(s"Table '$tableName' doesn't exist in database '$databaseName'")

    temp._1.isDefined
  }

  def execute(inputDataFrame: DataFrame): Either[Array[ (DataFrame, Any, Any) ], String] = {
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
                        Logger.log.error(either.right.get)
                    null
                    }
          )
      (failCount, Row.fromSeq(array))
    })
    val validRDD: RDD[(Int, Row)] = filterRdd(true, failedFieldLimit, temp)
    val invalidRDD: RDD[(Int, Row)] = filterRdd(false, failedFieldLimit, temp)
    val validCount: Int = validRDD.count.toInt
    val invalidCount: Int = invalidRDD.count.toInt

      //.filter(tuple => tuple._1 <= failedFieldLimit)
      //partition(tuple => tuple._1 <= failedFieldLimit)

    Logger.log.info(s"failed schema transformation rows count: $invalidCount")
    Logger.log.info(s"passed schema transformation rows count: $validCount")

    if(invalidCount > failedRowLimit)
      return Right(s"Aborting job because number of row failed($invalidCount) in schema transformation is greater then threshold($failedRowLimit)")

    val transformedRDD = validRDD.map(tuple => tuple._2)
    // val sparkContext: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
    // val transformedRDD = sparkContext.makeRDD(transformedSequence)

    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    val outputDF = sqlContext.createDataFrame(transformedRDD, schema = tableSchema)

    Left(Array( (outputDF, validCount, invalidCount) ))
  }

  def filterRdd(valid: Boolean, failedFieldLimit: Int, temp: RDD[(Int, Row)] ): RDD[(Int, Row)] = {
    if(valid) temp.filter(_._1 <= failedFieldLimit) else temp.filter(_._1 > failedFieldLimit)
  }
}