package com.abhioncbr.etlFramework.commons.extract

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.common.query.Query

case class Extract(feeds: Array[Feed])
case class Feed(feedName: String,
                extractionType: ExtractionType.valueType,
                dataPath: Option[FilePath], //fileInitialPath: String, fileNamePattern: String, formatFileName: Boolean, filePrefix: String,
                query: Option[Query], //dbPropertyFile: String, queryFilePath: String, queryParams: List[QueryParam],
                validateExtractedData: Boolean)
