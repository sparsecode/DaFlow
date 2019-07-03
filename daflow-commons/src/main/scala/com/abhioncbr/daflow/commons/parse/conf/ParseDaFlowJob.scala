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

package com.abhioncbr.daflow.commons.parse.conf

import com.abhioncbr.daflow.commons.Context
import com.abhioncbr.daflow.commons.ContextConstantEnum.HADOOP_CONF
import com.abhioncbr.daflow.commons.conf.DaFlowJobConf
import com.abhioncbr.daflow.commons.exception.DaFlowJobConfigException
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait ParseDaFlowJob {
  private val jobConfigErrorMessage: String => String =
    (msg: String) => { s"[Error-JobConfigFile]: Exception message: `$msg`" }

  def validate(specFilePath: String, configFilePath: String): Boolean
  def parse(configFilePath: String, loadFromHDFS: Boolean): Either[DaFlowJobConfigException, DaFlowJobConf]

  def getConfigFileContent(configFilePath: String, loadFromHDFS: Boolean): Either[DaFlowJobConfigException, String] = {
    getConfigFileReader(configFilePath, loadFromHDFS) match {
      case Left(exception) => Left(exception)
      case Right(reader) =>
        val xmlContent = Stream.continually(reader.readLine()).takeWhile(_ != null).toArray[String].mkString
        reader.close()
        Right(xmlContent)
    }
  }

  def getConfigFileReader(configFilePath: String, loadFromHDFS: Boolean): Either[DaFlowJobConfigException, BufferedReader] = {
    val result: Try[BufferedReader] = Try{
      val reader: BufferedReader = if (loadFromHDFS) {
        val fs = FileSystem.get(Context.getContextualObject[Configuration](HADOOP_CONF))
        new BufferedReader(new InputStreamReader(fs.open(new Path(configFilePath))))
      } else {
        new BufferedReader(new InputStreamReader(new FileInputStream(configFilePath)))
      }
      reader
    }

    result match {
      case Success(value) => Right(value)
      case Failure(exception) => Left(new DaFlowJobConfigException(jobConfigErrorMessage(exception.getMessage),
        configFilePath, Some(exception)))
    }
  }
}
