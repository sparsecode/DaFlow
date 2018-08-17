package com.abhioncbr.etlFramework.etl.feed.extractData

import java.io.{BufferedReader, InputStreamReader}

import com.abhioncbr.etlFramework.etl.feed.Logger
import com.abhioncbr.etlFramework.etl.feed.common.ContextConstantEnum._
import com.abhioncbr.etlFramework.etl.feed.common.{Context, Extract, QueryParam}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime

class ExtractDataFromHive(dbPropertyFile: String = Context.getContextualObject[Extract](EXTRACT).dbPropertyFile,
                        sqlQueryFile: String = Context.getContextualObject[Extract](EXTRACT).queryFilePath,
                        formatFileName: Boolean = Context.getContextualObject[Extract](EXTRACT).formatFileName,
                        sqlQueryParams: List[QueryParam] = Context.getContextualObject[Extract](EXTRACT).queryParams) extends ExtractData{

  def getRawData(firstDate: DateTime, secondDate: Option[DateTime]): DataFrame = {
    lazy val fs = FileSystem.get(new Configuration())

    //reading query from the query file.
    val tableQueryReader = new BufferedReader(new InputStreamReader(fs.open(new Path(sqlQueryFile))))
    val rawQuery = Stream.continually(tableQueryReader.readLine()).takeWhile(_ != null).toArray[String].mkString.stripMargin

    val queryParams = QueryParamTypeEnum.getParamsValue(sqlQueryParams)
    println("query param values" + queryParams.mkString(" , "))
    val tableQuery = String.format(rawQuery, queryParams:_*)
    Logger.log.info(s"going to execute hive query  \\n: $tableQuery")

    val sqlContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)
    sqlContext.sql(tableQuery)
  }
}
