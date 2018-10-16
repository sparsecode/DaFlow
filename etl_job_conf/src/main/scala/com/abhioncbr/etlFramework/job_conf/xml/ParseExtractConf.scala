package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.file.{FileNameParam, FilePath, PathInfixParam}
import com.abhioncbr.etlFramework.commons.common.query.{Query, QueryFileParam, QueryParam}
import com.abhioncbr.etlFramework.commons.util.FileUtil
import com.abhioncbr.etlFramework.commons.extract.{Extract, ExtractionType}

object ParseExtractConf {
  def fromXML(node: scala.xml.NodeSeq): Extract = {
    val extractionType = ExtractionType.getValueType((node \ "type").text)

    var extract: Extract = null
    if(extractionType.equals(ExtractionType.JSON)) {
      extract = Extract(extractionType,
        dataPath = Some(
          FilePath(pathPrefix = Some((node \ "file_initial_path").text),
            groupPatterns = None,
            feedPattern = Some(PathInfixParam((node \ "file_name_pattern").text, Some((node \ "format_file_name").text.toBoolean))),
            fileName = FileNameParam(Some((node \ "file_prefix").text))
          )
        ),
        query = None,
        validateExtractedData = (node \ "validate_extracted_data").text.toBoolean)
    } else if(extractionType.equals(ExtractionType.JDBC)) {
      extract = Extract(extractionType = extractionType,
        dataPath = None,
        query = Some(Query(
          QueryFileParam(Some(FileUtil.getFilePathObject((node \ "db_property_file_path").text)), Some(FileUtil.getFilePathObject((node \ "sql_query_file_path").text))),
          Some(Array[QueryParam]((node \ "query_params" \ "param").toList map { s => ParseQueryParam.fromXML(s) }: _*))
        )),
        validateExtractedData = (node \ "validate_extracted_data").text.toBoolean)
    }
    extract
  }
}