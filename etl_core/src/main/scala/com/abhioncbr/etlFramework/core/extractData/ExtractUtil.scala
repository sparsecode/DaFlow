package com.abhioncbr.etlFramework.core.extractData

import com.abhioncbr.etlFramework.commons.common.GeneralParam
import com.abhioncbr.etlFramework.commons.util.FileUtil

object ExtractUtil {
  def getParamsValue(paramList: List[GeneralParam]): Array[Object] ={
    paramList.map(queryParam => (queryParam.order, FileUtil.mapFormatArgs(Some(paramList.toArray)))).sortBy(_._1).map(_._2).toArray
  }
}
