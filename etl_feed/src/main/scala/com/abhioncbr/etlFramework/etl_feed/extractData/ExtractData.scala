package com.abhioncbr.etlFramework.etl_feed.extractData

import com.abhioncbr.etlFramework.commons.extract.Feed
import org.apache.spark.sql.DataFrame


trait ExtractData { def getRawData: DataFrame }