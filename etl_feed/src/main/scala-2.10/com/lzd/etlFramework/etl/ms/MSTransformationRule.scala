package com.lzd.etlFramework.etl.ms

import java.text.Normalizer

import com.lzd.etlFramework.etl.feed.common.Context
import com.lzd.etlFramework.etl.feed.common.ContextConstantEnum._
import com.lzd.etlFramework.etl.feed.loadData.hive.PartitionColumnTypeEnum
import com.lzd.etlFramework.etl.feed.transformData.TransformationRule
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

/**
  * Created by Abhishek on 28/9/17.
  */
class MSUtil {
  def registerUDF = {
    val sqlContext: SQLContext = Context.getContextualObject[SQLContext](SQL_CONTEXT)

    //count_bullets
    sqlContext.udf.register("count_bullets", (s: String) => "</li>".r.findAllMatchIn(s).length)

    //description_length
    sqlContext.udf.register("description_length", (s: String) => s.replaceAll("\\<.*?>", "").length)

    // Convert accented characters to their non accented form
    sqlContext.udf.register("normalize_unicode", (s: String) => Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}", ""))
  }
}

class MSTransformationRule extends TransformationRule{
  override def getOrder = order
  override def getGroup: Int = group
  override def condition(f: Int => DataFrame): Boolean = true
  override def execute(f: Int => DataFrame): Either[Array[(DataFrame, Any, Any)], String] = {
    import org.apache.spark.sql.functions.lit
    val value:Column = lit(PartitionColumnTypeEnum.getDataValue(columnValueType))

    val inputDataFrame:DataFrame = f.apply(group)

    Left(Array((inputDataFrame.withColumn(columnName,value), null, null) ))
  }
  override def toString: String = { s"ruleName = MSTransformationRule, order = $order, group = $group"}

}
