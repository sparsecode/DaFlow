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

package com.abhioncbr.daflow.jobConf.xml

class ParseETLJobXmlSpec extends XmlJobConfBase{
  val userDirectory: String = System.getProperty("user.dir")
  "validateXml" should "return true when valid xml file is provided as input" in {
    val xsdFile = s"$userDirectory/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"$userDirectory/etl_examples/example_job_xml/json_etl_example.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal None
    output should be (true)
  }

  "validateXml" should "return true when valid xml jdbc_template file is provided as input" in {
    val xsdFile = s"$userDirectory/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"$userDirectory/etl_examples/etl_job_xml_templates/extract_jdbc_import.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal None
    output should be (true)
  }

  "validateXml" should "return true when valid xml json_template file is provided as input" in {
    val xsdFile = s"$userDirectory/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"$userDirectory/etl_examples/etl_job_xml_templates/extract_json_import.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal None
    output should be (true)
  }

  "validateXml" should "return true when valid xml multiple_group_template file is provided as input" in {
    val xsdFile = s"$userDirectory/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"$userDirectory/etl_examples/etl_job_xml_templates/multiple_group_name.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal None
    output should be (true)
  }

  "validateXml" should "return true when valid xml multiple_transform_template file is provided as input" in {
    val xsdFile = s"$userDirectory/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"$userDirectory/etl_examples/etl_job_xml_templates/multiple_transform_rule.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal None
    output should be (true)
  }

}
