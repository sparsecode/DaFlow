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

package com.abhioncbr.daflow.job.conf.yaml.config

import cats.syntax.either._
import com.abhioncbr.daflow.commons.ProcessFrequencyEnum
import com.abhioncbr.daflow.commons.conf.JobStaticParamConf
import com.abhioncbr.daflow.commons.conf.common.FileNameParam
import com.abhioncbr.daflow.commons.conf.common.GeneralParamConf
import com.abhioncbr.daflow.commons.conf.common.PathInfixParam
import com.abhioncbr.daflow.commons.conf.extract.ExtractConf
import com.abhioncbr.daflow.commons.conf.extract.ExtractFeedConf
import com.abhioncbr.daflow.job.conf.yaml.config.YamlTags._
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class YamlJobConfig(jobStaticParam: JobStaticParamConf, extract: ExtractConf, transform: String, load: String)

object YamlJobConfig{
  implicit val decodeProcessFrequencyEnum: Decoder[ProcessFrequencyEnum.frequencyType] = Decoder
    .decodeString.emap {
      str => Either.catchNonFatal(ProcessFrequencyEnum.getProcessFrequencyEnum(str)).leftMap(t => t.getMessage)
    }

  implicit val decodeGeneralParamConf: Decoder[GeneralParamConf] = Decoder
    .forProduct4(ORDER, NAME, VALUE, DEFAULT_VALUE)(GeneralParamConf.apply)

  implicit val decodeJobStaticParamConf: Decoder[JobStaticParamConf] = Decoder
    .forProduct4(FREQUENCY, JOB_NAME, PUBLISH_STATS, OTHER_PARAMS)(JobStaticParamConf.apply)

  implicit val decodeFileNameParam: Decoder[FileNameParam] = Decoder.forProduct3(PREFIX, SUFFIX, SEPARATOR)(FileNameParam.apply)
  implicit val decodeParseFeedPattern: Decoder[PathInfixParam] = Decoder
    .forProduct4(0, FEED_NAME_PATTERN, FORMAT_FEED_NAME, FORMAT_ARG_VALUES)(PathInfixParam.apply)


  implicit val decodeExtractFeedConf: Decoder[ExtractFeedConf] = deriveDecoder[ExtractFeedConf]
  implicit val decodeYamlJobConfig: Decoder[YamlJobConfig] = deriveDecoder[YamlJobConfig]
}

