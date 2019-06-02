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
package com.abhioncbr.daflow.job.conf.xml

import com.abhioncbr.daflow.commons.conf.transform
import com.abhioncbr.daflow.commons.conf.transform.TransformConf
import com.abhioncbr.daflow.commons.conf.transform.TransformStepConf
import com.abhioncbr.daflow.job.conf.xml.AttributeTags._
import com.abhioncbr.daflow.job.conf.xml.NodeTags._

object ParseTransform {
  def fromXML(node: scala.xml.NodeSeq): TransformConf = {
    val steps: List[TransformStepConf] =
      List[TransformStepConf]((node \ STEP).toList map { s => ParseTransformStep.fromXML(s) }: _*)

    transform.TransformConf(transformSteps = steps,
      validateTransformedData = ParseUtil.parseBoolean((node \ VALIDATE_TRANSFORMED_DATA).text))
  }
}
