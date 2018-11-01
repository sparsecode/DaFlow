package com.abhioncbr.etlFramework.core.transformData

import org.apache.spark.sql.DataFrame

class TransformStep(order: Int, rules: Map[String, TransformRule]){
  override def toString: String = s" step order: $order, step rules: $rules"
  def getOrder: Int = order
  def getRules: Map[String, TransformRule] = rules

  var inputData: Map[String, DataFrame] = Map[String, DataFrame]()
  def getInputData(i: String) = inputData(i)
  lazy val requiredDF: Array[String] = rules.values.flatMap {
    case merge: MergeRule => val temp  = merge.asInstanceOf[MergeRule].getMergeGroup
      Array(temp._1, temp._2)
    case default =>Array(default.getGroup)
  }.toArray

  def addInputData(dataArray: Array[DataFrame]) : Either[Boolean, String] = {
    if (dataArray.length == requiredDF.length) { inputData =  requiredDF.zip(dataArray).toMap
      Left(true)
    } else Right(s"For transformation step ${this.getOrder}: input data frames size(${dataArray.length}) is not equal to rules map size(${rules.size})")
  }
}
