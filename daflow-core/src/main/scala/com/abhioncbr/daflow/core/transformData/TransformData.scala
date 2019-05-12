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

package com.abhioncbr.daflow.core.transformData

import com.abhioncbr.daflow.commons.ExecutionResult
import com.typesafe.scalalogging.Logger

class TransformData(transform : Transform) {
  private val logger = Logger(this.getClass)

  val test: (Either[Array[ExecutionResult], String], TransformStep) => Either[Array[ExecutionResult], String] = (input, step) => {
    input match {
      case Left(array) =>
        step.addInputData(array.map(res => res.resultDF)) match {
          case None =>

            // val stepOutput: ArrayBuffer[ExecutionResult] = new ArrayBuffer()
            val stepOutput: List[Either[Array[ExecutionResult], String]] = step.getRules.zipWithIndex.map(rule => {
              logger.info(s"step order: ${step.getOrder}, rule: $rule - checking condition")
              if (rule._1._2.condition(step.getInputData)) {
                logger.info(s"step order: ${step.getOrder}, rule: $rule - executing")
                rule._1._2.execute(step.getInputData) match {
                  case Left(outputArray) => Left(outputArray)
                  case Right(s) => Right(s)
                }
              } else {
                Right(s"For transformation step order: ${step.getOrder}, rule group:${rule._1._2.getGroup} : condition failed.")
              }
            }).toList

            val filteredStepOutput = stepOutput.filter(_.isRight)
            if(filteredStepOutput.nonEmpty) { Right(filteredStepOutput.mkString(" \\n ")) }
            else { Left(stepOutput.flatMap(_.left.get).toArray) }

          case Some(s) => Right(s)
        }

      case Right(e) => Right(e)
    }
  }

  def performTransformation(extractResult: Array[ExecutionResult]): Either[Array[ExecutionResult], String] = {
    val steps = transform.transformSteps
    val stepOutput: Either[Array[ExecutionResult], String] = Left(extractResult)

    val output: Either[Array[ExecutionResult], String] = steps.foldLeft(stepOutput)((c, n) => test(c, n))
    output
  }
}
