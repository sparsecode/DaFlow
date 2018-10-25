package com.abhioncbr.etlFramework.jobConf.xml

class ParseETLJobXmlSpec extends XmlJobConfBase{

  "validateXml" should "return true when valid xml file is provided as input" in {
    val xsdFile = s"${System.getProperty("user.dir")}/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"${System.getProperty("user.dir")}/etl_examples/example_job_xml/json_etl_example.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal null
    output should be (true)
  }

  "validateXml" should "return true when valid xml jdbc_template file is provided as input" in {
    val xsdFile = s"${System.getProperty("user.dir")}/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"${System.getProperty("user.dir")}/etl_examples/etl_job_xml_templates/extract_jdbc_import.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal null
    output should be (true)
  }

  "validateXml" should "return true when valid xml json_template file is provided as input" in {
    val xsdFile = s"${System.getProperty("user.dir")}/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"${System.getProperty("user.dir")}/etl_examples/etl_job_xml_templates/extract_json_import.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal null
    output should be (true)
  }

  "validateXml" should "return true when valid xml multiple_group_template file is provided as input" in {
    val xsdFile = s"${System.getProperty("user.dir")}/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"${System.getProperty("user.dir")}/etl_examples/etl_job_xml_templates/multiple_group_name.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal null
    output should be (true)
  }

  "validateXml" should "return true when valid xml multiple_transform_template file is provided as input" in {
    val xsdFile = s"${System.getProperty("user.dir")}/etl_job_conf/etl_feed_job.xsd"
    val xmlFile = s"${System.getProperty("user.dir")}/etl_examples/etl_job_xml_templates/multiple_transform_rule.xml"
    val parse: ParseETLJobXml = new ParseETLJobXml
    val output: Boolean = parse.validateXml(xsdFile, xmlFile)

    output should not equal null
    output should be (true)
  }

}
