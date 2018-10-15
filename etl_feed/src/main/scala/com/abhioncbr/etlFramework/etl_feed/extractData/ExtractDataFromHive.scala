package com.abhioncbr.etlFramework.etl_feed.extractData

import java.io.{BufferedReader, InputStreamReader}

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum._
import com.abhioncbr.etlFramework.commons.extract.Extract
import com.abhioncbr.etlFramework.commons.Logger
import com.abhioncbr.etlFramework.commons.common.query.{Query, QueryParam, QueryParamTypeEnum}
import com.abhioncbr.etlFramework.commons.util.FileUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}

class ExtractDataFromHive(query: Option[Query] = Context.getContextualObject[Extract](EXTRACT).query) extends AbstractExtractData{

  def getRawData: DataFrame = {
    lazy val fs = FileSystem.get(new Configuration())

    //reading query from the query file.
    val sqlQueryFile: String  = FileUtil.getFilePathString(query.get.queryFile.queryFile.get)
    val tableQueryReader = new BufferedReader(new InputStreamReader(fs.open(new Path(sqlQueryFile))))
    val rawQuery = Stream.continually(tableQueryReader.readLine()).takeWhile(_ != null).toArray[String].mkString.stripMargin

    val sqlQueryParams: Array[QueryParam] = query.get.queryArgs.get
    val queryParams = QueryParamTypeEnum.getParamsValue(sqlQueryParams.toList)
    println("query param values" + queryParams.mkString(" , "))
    val tableQuery = String.format(rawQuery, queryParams:_*)
    Logger.log.info(s"going to execute hive query  \\n: $tableQuery")

    val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    sqlContext.sql(tableQuery)
  }
}
