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

package com.abhioncbr.daflow.core.validateData

abstract class ValidateTransformedData extends ValidateData
  /* private val logger = Logger(this.getClass)
  private val sparkContext: SparkContext = Context.getContextualObject[SparkContext](SPARK_CONTEXT)
  private val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)

  private val tableName = Context.getContextualObject[LoadFeedConf](LOAD_CONF).attributesMap("tableName")
  private val databaseName = Context.getContextualObject[LoadFeedConf](LOAD_CONF).attributesMap("databaseName")
  val partitionColumns: List[String] = Context.getContextualObject[LoadFeedConf](LOAD_CONF).partitioningData.get.
  partitionColumns.map(column => column.paramName)

  def validateSchema(dataFrame: DataFrame): (Boolean, Option[StructType], Option[StructType]) = {
    logger.info("Validating data frame schema and hive table schema")

    val dataFrameSchema = dataFrame.schema

    var tableSchema = Context.getContextualObject[(Option[StructType], Option[StructType])](SCHEMA)
    if(tableSchema == null)
      tableSchema = TransformUtil.tableMetadata(tableName, databaseName, sqlContext, partitionColumns)

    val output = if(tableSchema._1.isDefined) tableSchema._1.get == dataFrameSchema else false
    (output, tableSchema._1, Some(dataFrameSchema))
  }

  def validateData(dataFrame: DataFrame, structType: StructType, first: Any, second: Any):
  Array[(DataFrame, DataFrame, Any, Any)] ={
    logger.info("Validating data frame row schema and hive table schema")

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
  } */
