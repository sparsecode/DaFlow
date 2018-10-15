package com.abhioncbr.etlFramework.etl_feed.extractData

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.extract.Extract
import com.abhioncbr.etlFramework.commons.Logger
import com.abhioncbr.etlFramework.commons.common.query.{Query, QueryParam, QueryParamTypeEnum}
import com.abhioncbr.etlFramework.commons.util.FileUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}

class ExtractDataFromDB(query: Option[Query] = Context.getContextualObject[Extract](EXTRACT).query) extends AbstractExtractData{

  def getRawData: DataFrame = {
    lazy val fs = FileSystem.get(new Configuration())

    //reading database properties from property file.
    val propertyFilePath = FileUtil.getFilePathString(query.get.queryFile.configurationFile.get)
    Logger.log.info(s"db property file path: $propertyFilePath")

    val connectionProps = new Properties()
    connectionProps.load(fs.open(new Path(propertyFilePath)))
    val dbUri = connectionProps.getProperty("dburi")

    //reading query from the query file.
    val sqlQueryFile = FileUtil.getFilePathString(query.get.queryFile.queryFile.get)
    val tableQueryReader = new BufferedReader(new InputStreamReader(fs.open(new Path(sqlQueryFile))))
    val rawQuery = Stream.continually(tableQueryReader.readLine()).takeWhile(_ != null).toArray[String].mkString.stripMargin

    val sqlQueryParams: Array[QueryParam] = query.get.queryArgs.get
    val queryParams = QueryParamTypeEnum.getParamsValue(sqlQueryParams.toList)
    println("query param values" + queryParams.mkString(" , "))
    val tableQuery = String.format(rawQuery, queryParams:_*)
    Logger.log.info(s"going to execute jdbc query  \\n: $tableQuery")

    val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    sqlContext.read.jdbc(url = dbUri, table = tableQuery, properties = connectionProps)
  }
}
