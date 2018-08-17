package com.abhioncbr.etlFramework.etl.feed.transformData

import com.abhioncbr.etlFramework.etl.feed.Logger
import com.abhioncbr.etlFramework.etl.feed.common.Transform
import org.apache.spark.sql.DataFrame

class TransformationStep(order: Int, rules: Map[Int, TransformationRule]){
  override def toString: String = s" step order: $order, step rules: $rules"
  def getOrder = order
  def getRules = rules

  var inputData: Map[Int, DataFrame] = Map[Int, DataFrame]()
  def getInputData(i: Int) = inputData(i)
  lazy val requiredDF = rules.values.flatMap {
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

class TransformData(transform : Transform) {
  def performTransformation(rawDataFrame: DataFrame): Either[Array[(DataFrame, Any, Any)], String] = {
    val steps = transform.transformationSteps
    var stepOutput: Array[(DataFrame, Any, Any)] = Array((rawDataFrame, null, null))

    //iterating over transformation steps
    steps.foreach(step => {
      //setting up input data frames in transformation step
      step.addInputData(stepOutput.unzip3._1.toArray) match {
        //iterating for each group of transformation rules
        case Left(b) =>
          stepOutput = Array()
          step.getRules.zipWithIndex.foreach(rule => {
            Logger.log.info(s"step order: ${step.getOrder}, rule: $rule - checking condition")
            if (rule._1._2.condition(step.getInputData)) {
              Logger.log.info(s"step order: ${step.getOrder}, rule: $rule - executing")
              rule._1._2.execute(step.getInputData) match {
                case Left(array) => stepOutput = stepOutput ++ array
                case Right(s) => return Right(s)
              }
            } else
              return Right(s"For transformation step order: ${step.getOrder}, rule order: ${rule._1._2.getOrder}, rule group:${rule._1._2.getGroup} : condition failed.")
          })
        case Right(e) => return Right(e)
      }
    })
    Left(stepOutput)
  }
}