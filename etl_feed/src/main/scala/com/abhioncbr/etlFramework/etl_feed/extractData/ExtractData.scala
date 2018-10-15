package com.abhioncbr.etlFramework.etl_feed.extractData

import org.apache.spark.sql.DataFrame


trait ExtractData { def getRawData: DataFrame }