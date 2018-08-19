package com.abhioncbr.etlFramework.etl_feed.extractData

import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

/**
  * Created by Abhishek on 22/2/17.
  */
trait ExtractData { def getRawData(firstDate: DateTime, secondDate: Option[DateTime]): DataFrame }