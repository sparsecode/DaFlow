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

import com.abhioncbr.daflow.commons.conf.DaFlowJobConf
import com.abhioncbr.daflow.commons.exception.DaFlowJobConfigException
import com.abhioncbr.daflow.commons.parse.conf.ParseDaFlowJob
import com.abhioncbr.daflow.job.conf.xml.NodeTags._
import java.io._
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import scala.util.Try
import scala.xml.Node
import scala.xml.XML

object DaFlowJob{
  def fromXML(node: scala.xml.NodeSeq): DaFlowJobConf = {
    DaFlowJobConf(ParseJobStaticParam.fromXML(node \ JOB_STATIC_PARAM),
      ParseExtract.fromXML(node \ EXTRACT),
      ParseTransform.fromXML(node \ TRANSFORM),
      ParseLoad.fromXML(node \ LOAD)
    )
  }
}

class ParseDaFlowJobXml extends ParseDaFlowJob {
  val unknownXMLEntity: String = "Unknown entity found instead of '<DaFlowJob>'"

  def parse(configFilePath: String, loadFromHDFS: Boolean): Either[DaFlowJobConfigException, DaFlowJobConf] = {
    getConfigFileContent(configFilePath, loadFromHDFS) match {
      case Left(exception) => Left(exception)
      case Right(xmlContent) => parseNode(XML.loadString(xmlContent)) match {
        case Left(msg) => Left(new DaFlowJobConfigException(msg, configFilePath))
        case Right(daFlowJobConfig) => Right(daFlowJobConfig)
      }
    }
  }

  def validate(specFilePath: String, configFilePath: String): Boolean = {
    Try({
      val factory: SchemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      val schema = factory.newSchema(new StreamSource(new FileInputStream(specFilePath)))
      schema.newValidator().validate(new StreamSource(new FileInputStream(configFilePath)))
      true
    }).getOrElse(false)
  }

  private def parseNode(node: scala.xml.Node): Either[String, DaFlowJobConf] = {
    val trimmedNode: Node = scala.xml.Utility.trim(node)
    trimmedNode match {
      case <DaFlowJob>{_*}</DaFlowJob> => val daFlowJobConfig = DaFlowJob.fromXML(trimmedNode)
        Right(daFlowJobConfig)
      case _ => Left(unknownXMLEntity)
    }
  }
}
