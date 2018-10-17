package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.file.FilePath
import com.abhioncbr.etlFramework.commons.common.query.{Query, QueryFileParam, QueryParam}
import com.abhioncbr.etlFramework.commons.extract.{Extract, ExtractionType}

object ParseExtractConf {
  def fromXML(node: scala.xml.NodeSeq): Extract = {
    val extractionType = ExtractionType.getValueType((node \ "type").text)
    val extract: Extract = Extract(extractionType = extractionType,
        dataPath =  Some(
          FilePath(pathPrefix = Some((node \ "file_initial_path").text),
            groupPatterns = None,
            feedPattern = None, //Some(PathInfixParam((node \ "file_name_pattern").text, Some((node \ "format_file_name").text.toBoolean))),
            fileName = None //Some(FileNameParam(Some((node \ "file_prefix").text)))
          )
        ),
        query = Some(Query(
          queryFile = QueryFileParam(configurationFile= ParseUtil.parseFilePathString((node \ "db_property_file_path").text),
            queryFile= ParseUtil.parseFilePathString((node \ "sql_query_file_path").text)),
          queryArgs = Some(Array[QueryParam]((node \ "query_params" \ "param").toList map { s => ParseQueryParam.fromXML(s) }: _*))
        )),
        validateExtractedData = ParseUtil.parseBoolean((node \ "validate_extracted_data").text))
    extract
  }
}