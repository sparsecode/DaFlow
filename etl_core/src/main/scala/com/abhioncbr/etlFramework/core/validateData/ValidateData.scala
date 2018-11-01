package com.abhioncbr.etlFramework.core.validateData

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait ValidateData{
  def validateSchema(dataFrame: DataFrame) : (Boolean, Option[StructType], Option[StructType])
  def validateData(dataFrame: DataFrame, structType: StructType, first: Any, second: Any): Array[(DataFrame, DataFrame, Any, Any)]
}
