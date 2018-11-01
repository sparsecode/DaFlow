package com.abhioncbr.etlFramework.core.loadData

import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

trait LoadData{
  def loadTransformedData(dataFrame: DataFrame, date: Option[DateTime]): Either[Boolean, String]
}

