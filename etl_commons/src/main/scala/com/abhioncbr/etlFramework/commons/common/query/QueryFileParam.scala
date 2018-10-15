package com.abhioncbr.etlFramework.commons.common.query

import com.abhioncbr.etlFramework.commons.common.file.FilePath

case class QueryFileParam(configurationFile: Option[FilePath], queryFile: Option[FilePath])
