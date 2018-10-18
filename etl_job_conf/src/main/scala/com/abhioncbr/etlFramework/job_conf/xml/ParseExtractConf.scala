package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.common.query.{Query, QueryFileParam}
import com.abhioncbr.etlFramework.commons.extract.{Extract, ExtractionType, Feed}

object ParseExtractConf {
  def fromXML(node: scala.xml.NodeSeq): Extract = {
    val extract: Extract = Extract(feeds = Array[Feed]((node \ "feed" ).toList map { s => ParseFeedConf.fromXML(s) }: _*))
    extract
  }
}

object ParseFeedConf {
  def fromXML(node: scala.xml.NodeSeq): Feed = {
    val feed: Feed = Feed(feedName = (node \ "feed_name").text,
      extractionType = ExtractionType.getValueType((node \ "type").text),
      dataPath =  Some(ParseDataPath.fromXML(node \ "data_path")),
      query = Some(Query(
        queryFile = QueryFileParam(configurationFile= ParseUtil.parseFilePathString((node \ "db_property_file_path").text),
          queryFile= ParseUtil.parseFilePathString((node \ "sql_query_file_path").text)),
        queryArgs = Some(Array[GeneralParam]((node \ "query_params" \ "param").toList map { s => ParseGeneralParams.fromXML(s) }: _*))
      )),
      validateExtractedData = ParseUtil.parseBoolean((node \ "validate_extracted_data").text))
    feed
  }
}