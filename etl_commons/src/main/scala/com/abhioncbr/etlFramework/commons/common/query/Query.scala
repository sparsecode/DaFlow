package com.abhioncbr.etlFramework.commons.common.query

import com.abhioncbr.etlFramework.commons.common.GeneralParam

case class Query(queryFile: QueryFileParam, queryArgs: Option[Array[GeneralParam]])
