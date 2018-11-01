package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.{DataPath, QueryConf}
import com.abhioncbr.etlFramework.commons.extract.{ExtractConf, ExtractFeedConf, ExtractionType}

object ParseExtract {
  def fromXML(node: scala.xml.NodeSeq): ExtractConf = {
    val extract: ExtractConf = ExtractConf(feeds = Array[ExtractFeedConf]((node \ "feed" ).toList map { s => ParseExtractFeed.fromXML(s) }: _*))
    extract
  }
}

object ParseExtractFeed {
  def fromXML(node: scala.xml.NodeSeq): ExtractFeedConf = {
    val feedName: String = (node \ "@feedName").text
    val extractionSubType: String = (node \ "_").head.attributes.value.text.toUpperCase
    val validateExtractedData: Boolean = ParseUtil.parseBoolean((node \ "@validateExtractedData").text)
    val extractionType: ExtractionType.valueType = ExtractionType.getValueType( valueTypeString = (node \ "_").head.label.toUpperCase)

    val query: Option[QueryConf] = ParseUtil.parseNode[QueryConf](node \ "jdbc" \ "query", None, ParseQuery.fromXML)
    val dataPath: Option[DataPath] = ParseUtil.parseNode[DataPath](node \ "fileSystem" \ "dataPath", None, ParseDataPath.fromXML)

    val feed: ExtractFeedConf = ExtractFeedConf(extractFeedName = feedName,
      extractionType = extractionType, extractionSubType = extractionSubType,
      dataPath = dataPath , query =query, validateExtractedData = validateExtractedData)
    feed
  }
}