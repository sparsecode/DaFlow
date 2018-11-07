/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.abhioncbr.etlFramework.core.transformData

import org.apache.spark.sql.DataFrame

class TransformStep(order: Int, rules: Map[String, TransformRule]){
  override def toString: String = s" step order: $order, step rules: $rules"
  def getOrder: Int = order
  def getRules: Map[String, TransformRule] = rules

  val inputData: scala.collection.mutable.Map[String, DataFrame] = scala.collection.mutable.Map[String, DataFrame]()
  def getInputData(i: String): DataFrame = inputData(i)

  lazy val requiredDF: Array[String] = rules.values.flatMap {
    case merge: MergeRule =>
      val temp = merge.asInstanceOf[MergeRule].getMergeGroup
      Array(temp._1, temp._2)
    case default: Any => Array(default.getGroup)
  }.toArray

  def addInputData(dataArray: Array[DataFrame]) : Either[Boolean, String] = {
    if (dataArray.length == requiredDF.length) {
      inputData.clear
      inputData ++= requiredDF.zip(dataArray).toMap
      Left(true)
    } else {
      Right(s"For transformation step ${this.getOrder}: input data frames size(${dataArray.length}) " +
        s"is not equal to rules map size(${rules.size})")
    }
  }
}
