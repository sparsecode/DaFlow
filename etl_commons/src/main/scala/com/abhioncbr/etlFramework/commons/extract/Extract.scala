package com.abhioncbr.etlFramework.commons.extract

case class Extract(extractionType: ExtractionType.valueType, fileInitialPath: String, fileNamePattern: String, formatFileName: Boolean,
                   filePrefix: String, dbPropertyFile: String, queryFilePath: String, queryParams: List[QueryParam], validateExtractedData: Boolean)
