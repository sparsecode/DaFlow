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

package com.abhioncbr.etlFramework.jobConf.xml

import java.io._
import javax.xml.XMLConstants

import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.ContextConstantEnum.HADOOP_CONF
import com.abhioncbr.etlFramework.commons.NotificationMessages.{exceptionMessage => EM}
import com.abhioncbr.etlFramework.commons.NotificationMessages.{unknownXMLEntity => UE}
import com.abhioncbr.etlFramework.commons.NotificationMessages.{jobXmlFileDoesNotExist => JXF}
import com.abhioncbr.etlFramework.commons.NotificationMessages.{exceptionWhileParsing => EWP}
import com.abhioncbr.etlFramework.commons.extract.ExtractConf
import com.abhioncbr.etlFramework.commons.job.ETLJobConf
import com.abhioncbr.etlFramework.commons.job.JobStaticParamConf
import com.abhioncbr.etlFramework.commons.load.LoadConf
import com.abhioncbr.etlFramework.commons.transform.TransformConf
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.util.Try
import scala.xml.Node

object ETLJob{
  def fromXML(node: scala.xml.NodeSeq): ETLJobConf = {
  ETLJobConf(ParseJobStaticParam.fromXML(node \ "jobStaticParam"), ParseExtract.fromXML(node \ "extract"),
  ParseTransform.fromXML(node \ "transform"), ParseLoad.fromXML(node \ "load"))
  }
}

class ParseETLJobXml {
  def parseXml(path: String, loadFromHDFS: Boolean): Either[String, String] = {
    try {
      val reader: BufferedReader = if (loadFromHDFS) {
        val fs = FileSystem.get(Context.getContextualObject[Configuration](HADOOP_CONF))
        new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      } else { new BufferedReader(new InputStreamReader(new FileInputStream(path))) }

      val lines = Stream.continually(reader.readLine()).takeWhile(_ != null).toArray[String].mkString
      reader.close()
      Left(lines)
    } catch {
      case fileNotFoundException: FileNotFoundException => Right(s"${JXF(path)}. ${EM(fileNotFoundException)}".stripMargin)
      case exception: Exception => Right(s"$EWP ${EM(exception)}".stripMargin)
    }
  }

  def validateXml(xsdFile: String, xmlFile: String): Boolean = {
    Try({
      val factory: SchemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      val schema = factory.newSchema(new StreamSource(new FileInputStream(xsdFile)))
      schema.newValidator().validate(new StreamSource(new FileInputStream(xmlFile)))
      true
    }).getOrElse(false)
}

  def parseNode(node: scala.xml.Node): Either[(JobStaticParamConf, ExtractConf, TransformConf, LoadConf), String] = {
    val trimmedNode: Node = scala.xml.Utility.trim(node)
    trimmedNode match {
      case <etlJob>{ c @ _* }</etlJob> => val etlJob = ETLJob.fromXML(trimmedNode)
        Left((etlJob.jobStaticParam, etlJob.extract, etlJob.transform, etlJob.load))
      case _ => Right(UE)
    }
  }
}

