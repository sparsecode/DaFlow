package com.abhioncbr.etlFramework.jobConf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.common.query.{QueryObject, QueryFilesParam}
import com.abhioncbr.etlFramework.commons.extract.{Extract, ExtractionType, Feed}

object ParseExtractConf {
  def fromXML(node: scala.xml.NodeSeq): Extract = {
    val extract: Extract = Extract(feeds = Array[Feed]((node \ "feed" ).toList map { s => ParseFeedConf.fromXML(s) }: _*))
    extract
  }
}

object ParseFeedConf {
  def fromXML(node: scala.xml.NodeSeq): Feed = {
    val feedName: String =(node \ "@feedName").text
    val dataPath: Option[FilePath] = ParseDataPath.fromXML(node \ "fileSystem" \ "dataPath")
    val query: Option[QueryObject] = ParseQuery.fromXML(node \ "jdbc" \ "query")
    val validateExtractedData: Boolean = ParseUtil.parseBoolean((node \ "@validateExtractedData").text)


    val feed: Feed = Feed(feedName = feedName,
      extractionType = ExtractionType.getValueType((node \ "type").text),
      dataPath = dataPath , query =query, validateExtractedData = validateExtractedData)
    feed
  }
}