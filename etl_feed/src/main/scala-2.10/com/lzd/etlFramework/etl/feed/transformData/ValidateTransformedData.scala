package com.lzd.etlFramework.etl.feed.transformData

import com.lzd.etlFramework.etl.feed.common.{Context, Load}
import com.lzd.etlFramework.etl.feed.common.ContextConstantEnum._
import com.lzd.etlFramework.etl.feed.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


trait ValidateTransformedData{
  def validateSchema(dataFrame: DataFrame) : (Boolean, Option[StructType], Option[StructType])
  def validateData(dataFrame: DataFrame, structType: StructType, first: Any, second: Any): Array[(DataFrame, DataFrame, Any, Any)]
}

class ValidateTransformedDataSchema extends ValidateTransformedData {
  private val sparkContext: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
  private val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
  private val hiveContext: HiveContext = Context.getContextualObject[HiveContext](HIVE_CONTEXT)

  private val tableName = Context.getContextualObject[Load](LOAD).tableName
  private val databaseName = Context.getContextualObject[Load](LOAD).dbName
  val partitionColumns: List[String] = Context.getContextualObject[Load](LOAD).partData.partitionColumns.map(column => column.columnName)

  def validateSchema(dataFrame: DataFrame): (Boolean, Option[StructType], Option[StructType]) = {
    Logger.log.info("Validating data frame schema and hive table schema")

    val dataFrameSchema = dataFrame.schema


    var tableSchema = Context.getContextualObject[(Option[StructType], Option[StructType])](SCHEMA)
    if(tableSchema == null)
      tableSchema = TransformUtil.tableMetadata(tableName, databaseName, hiveContext, partitionColumns)

    val output = if(tableSchema._1.isDefined) tableSchema._1.get == dataFrameSchema else false
    (output, tableSchema._1, Some(dataFrameSchema))
  }

  def validateData(dataFrame: DataFrame, structType: StructType, first: Any, second: Any): Array[(DataFrame, DataFrame, Any, Any)] ={
    Logger.log.info("Validating data frame row schema and hive table schema")

    //val temp1 = dataFrame.collect
    //val temp = temp1.partition(row => compareSchema( row, structType))
    //val validatedRdd = sparkContext.parallelize(temp._1)
    val validatedDataFrame = sqlContext.createDataFrame(dataFrame.rdd.filter(_.schema == structType), structType)

    //val nonValidatedRdd = sparkContext.parallelize(temp._2)
    val nonValidatedDataFrame = sqlContext.createDataFrame(dataFrame.rdd.filter(_.schema != structType), structType)

    Array((validatedDataFrame,nonValidatedDataFrame, first, second))
  }

  def compareSchema(row: Row, structType: StructType): Boolean = {
    try{ row.schema == structType }
    catch { case e: Throwable => println(row.mkString); false }
  }
}