package com.abhioncbr.etlFramework.core.extractData

import com.abhioncbr.etlFramework.commons.extract.ExtractFeedConf
import org.apache.spark.sql.DataFrame


trait ExtractData { def getRawData: DataFrame }