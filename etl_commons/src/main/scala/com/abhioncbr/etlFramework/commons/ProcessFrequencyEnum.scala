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

package com.abhioncbr.etlFramework.commons

object ProcessFrequencyEnum extends Enumeration {
  type frequencyType = Value
  val ONCE, HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY, DATE_RANGE = Value

  def getProcessFrequencyEnum(frequencyString: String): ProcessFrequencyEnum.frequencyType = {
    val processFrequencyEnum = frequencyString match {
      case "ONCE" => ProcessFrequencyEnum.ONCE
      case "HOURLY" => ProcessFrequencyEnum.HOURLY
      case "DAILY" => ProcessFrequencyEnum.DAILY
      case "WEEKLY" => ProcessFrequencyEnum.WEEKLY
      case "MONTHLY" => ProcessFrequencyEnum.MONTHLY
      case "YEARLY" => ProcessFrequencyEnum.YEARLY
      case "DATE_RANGE" => ProcessFrequencyEnum.DATE_RANGE
      case _ => throw new RuntimeException(s"'$frequencyString', process frequency not supported.")
    }
    processFrequencyEnum
  }
}
