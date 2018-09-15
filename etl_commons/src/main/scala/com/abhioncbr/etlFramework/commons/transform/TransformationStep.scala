package com.abhioncbr.etlFramework.commons.transform

import org.apache.spark.sql.DataFrame

class TransformationStep(order: Int, rules: Map[Int, TransformationRule]){
  override def toString: String = s" step order: $order, step rules: $rules"
  def getOrder: Int = order
  def getRules: Map[Int, TransformationRule] = rules

  var inputData: Map[Int, DataFrame] = Map[Int, DataFrame]()
  def getInputData(i: Int) = inputData(i)
  lazy val requiredDF: Array[Int] = rules.values.flatMap {
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
