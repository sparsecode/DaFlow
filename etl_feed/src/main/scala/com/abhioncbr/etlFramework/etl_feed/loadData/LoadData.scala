package com.abhioncbr.etlFramework.etl_feed.loadData

import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

trait LoadData{
  def loadTransformedData(dataFrame: DataFrame, date: DateTime): Either[Boolean, String]
}

