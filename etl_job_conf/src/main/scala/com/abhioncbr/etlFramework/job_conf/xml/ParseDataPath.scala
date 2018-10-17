package com.abhioncbr.etlFramework.job_conf.xml

import com.abhioncbr.etlFramework.commons.common.file.{FileNameParam, FilePath}

object ParseDataPath {
  def fromXML(node: scala.xml.NodeSeq): FilePath = {
    val dataPath: FilePath = FilePath(pathPrefix = Some((node \ "initial_path").text),
        groupPatterns = None,
        feedPattern = None, //Some(PathInfixParam((node \ "file_name_pattern").text, Some((node \ "format_file_name").text.toBoolean))),
        fileName = Some(ParseFileName.fromXML(node \ "file_name"))
    )
    dataPath
  }
}

object ParseFileName {
  def fromXML(node: scala.xml.NodeSeq): FileNameParam = {
    val fileName: FileNameParam = FileNameParam(fileNamePrefix = Some((node \ "file_prefix").text),
      fileNameSuffix = Some((node \ "File_suffix").text))
      fileName
   }
}
