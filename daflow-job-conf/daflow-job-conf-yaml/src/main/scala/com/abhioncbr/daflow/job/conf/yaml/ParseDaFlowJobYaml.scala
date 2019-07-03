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

package com.abhioncbr.daflow.job.conf.yaml

import com.abhioncbr.daflow.commons.conf.DaFlowJobConf
import com.abhioncbr.daflow.commons.exception.DaFlowJobConfigException
import com.abhioncbr.daflow.commons.parse.conf.ParseDaFlowJob
import com.abhioncbr.daflow.job.conf.yaml.config.YamlJobConfig
import io.circe.DecodingFailure
import io.circe.Json
import io.circe.ParsingFailure
import io.circe.yaml.parser

class ParseDaFlowJobYaml extends ParseDaFlowJob{

  def parse(configFilePath: String, loadFromHDFS: Boolean): Either[DaFlowJobConfigException, DaFlowJobConf] = {
    parseYaml(configFilePath, loadFromHDFS) match {
      case Left(exception: DaFlowJobConfigException) => Left(exception)

      case Right(config: YamlJobConfig) =>
        Right(DaFlowJobConf(null, null, null, null))
    }
  }

  def parseYaml(configFilePath: String, loadFromHDFS: Boolean): Either[DaFlowJobConfigException, YamlJobConfig] = {
    getConfigFileReader(configFilePath, loadFromHDFS) match {
      case Left(exception: DaFlowJobConfigException) => Left(exception)

      case Right(yamlReader) =>
        parser.parse(yamlReader) match {

          case Right(parseConfig: Json) => parseConfig.as[YamlJobConfig] match {
            case Right(config: YamlJobConfig) => yamlReader.close()
              Right(config)

            case Left(exception: DecodingFailure) =>
              Left(new DaFlowJobConfigException(exception.getMessage, configFilePath, Some(exception)))
          }

        case Left(exception: ParsingFailure) =>
          Left(new DaFlowJobConfigException(exception.getMessage, configFilePath, Some(exception)))
      }
    }
  }

  def validate(specFilePath: String, configFilePath: String): Boolean = {
    true
  }
}
