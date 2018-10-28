package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.common.query.QueryObject
import com.abhioncbr.etlFramework.commons.extract.{Extract, ExtractionType, Feed}

object ParseExtractConf {
  def fromXML(node: scala.xml.NodeSeq): Extract = {
    val extract: Extract = Extract(feeds = Array[Feed]((node \ "feed" ).toList map { s => ParseFeedConf.fromXML(s) }: _*))
    extract
  }
}

object ParseFeedConf {
  def fromXML(node: scala.xml.NodeSeq): Feed = {
    val feedName: String = (node \ "@feedName").text
    val dataPath: Option[FilePath] = ParseUtil.parseNode[FilePath](node \ "fileSystem" \ "dataPath", None, ParseDataPath.fromXML)
    val query: Option[QueryObject] = ParseUtil.parseNode[QueryObject](node \ "jdbc" \ "query", None, ParseQuery.fromXML)
    val validateExtractedData: Boolean = ParseUtil.parseBoolean((node \ "@validateExtractedData").text)

    val extractionType: ExtractionType.valueType = if(dataPath.isDefined)
      ExtractionType.getValueType( valueTypeString = "FILE_SYSTEM")
    else if(query.isDefined)
      ExtractionType.getValueType( valueTypeString = (node \ "jdbc" \ "@type").text )
    else ExtractionType.UNSUPPORTED

    val feed: Feed = Feed(feedName = feedName,
      extractionType = extractionType,
      dataPath = dataPath , query =query, validateExtractedData = validateExtractedData)
    feed
  }
}