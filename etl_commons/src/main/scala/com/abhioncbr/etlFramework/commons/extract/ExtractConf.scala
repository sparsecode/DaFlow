package com.abhioncbr.etlFramework.commons.extract

import com.abhioncbr.etlFramework.commons.common.QueryObject
import com.abhioncbr.etlFramework.commons.common.file.DataPath

case class ExtractConf(feeds: Array[ExtractFeedConf])
case class ExtractFeedConf(extractFeedName: String,
                           extractionType: ExtractionType.valueType,
                           extractionSubType: String,
                           dataPath: Option[DataPath], //fileInitialPath: String, fileNamePattern: String, formatFileName: Boolean, filePrefix: String,
                           query: Option[QueryObject], //dbPropertyFile: String, queryFilePath: String, queryParams: List[QueryParam],
                           validateExtractedData: Boolean)
