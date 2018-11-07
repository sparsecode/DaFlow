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

import com.abhioncbr.etlFramework.commons.ExecutionResult
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame

class TransformData(transform : Transform) {
  private val logger = Logger(this.getClass)

  def performTransformation(extractResult: Array[ExecutionResult]): Either[Array[ExecutionResult], String] = {
    val steps = transform.transformSteps
    var stepOutput: Array[ExecutionResult] = extractResult

    // iterating over transformation steps
    steps.foreach(step => {
      // setting up input data frames in transformation step
      step.addInputData(stepOutput.map(res => res.resultDF)) match {
        // iterating for each group of transformation rules
        case Left(b) =>
          stepOutput = Array()
          step.getRules.zipWithIndex.foreach(rule => {
            logger.info(s"step order: ${step.getOrder}, rule: $rule - checking condition")
            if (rule._1._2.condition(step.getInputData)) {
              logger.info(s"step order: ${step.getOrder}, rule: $rule - executing")
              rule._1._2.execute(step.getInputData) match {
                case Left(array) => stepOutput = stepOutput ++ array
                case Right(s) => return Right(s)
              }
            } else {
              return Right(s"For transformation step order: ${step.getOrder}, " +
                s"rule group:${rule._1._2.getGroup} : condition failed.")
            }
          })
        case Right(e) => return Right(e)
      }
    })
    Left(stepOutput)
  }

}