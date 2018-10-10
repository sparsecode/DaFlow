package com.abhioncbr.etlFramework.commons.transform

import org.apache.spark.sql.DataFrame

case class TransformationResult(resultDF: DataFrame, resultInfo1: Any, resultInfo2: Any) { }