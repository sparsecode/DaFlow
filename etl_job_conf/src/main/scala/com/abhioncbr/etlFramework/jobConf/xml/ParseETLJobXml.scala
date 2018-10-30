package com.abhioncbr.etlFramework.jobConf.xml

import java.io._

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.HADOOP_CONF
import com.abhioncbr.etlFramework.commons.extract.Extract
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.job.{ETLJob, JobStaticParam}
import com.abhioncbr.etlFramework.commons.load.{Load, LoadFeed}
import com.abhioncbr.etlFramework.commons.transform.Transform
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try

object ETLJob{
  def fromXML(node: scala.xml.NodeSeq): ETLJob = {
      new ETLJob(ParseJobStaticParam.fromXML(node \ "jobStaticParam"),
        ParseExtract.fromXML(node \ "extract"),
        ParseTransform.fromXML(node \ "transform"),
        ParseLoad.fromXML(node \ "load")
    )
  }
}

class ParseETLJobXml {

  def parseXml(path: String, loadFromHdfs: Boolean): Either[String, String] = {
    try {
      var reader: BufferedReader = null

      if (loadFromHdfs) {
        val fs = FileSystem.get(Context.getContextualObject[Configuration](HADOOP_CONF))
        reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      } else {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
      }

      val lines = Stream.continually(reader.readLine()).takeWhile(_ != null).toArray[String].mkString
      reader.close()
      Left(lines)
    } catch {
      case e: FileNotFoundException => Right(s"Not able to load job xml file. Provided path: '$path'. Exception message: ${e.getMessage}" )
      case ex: Exception => Right(s"Exception while parsing job xml file. Please validate xml. Exception message ${ex.getMessage}")
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

  def parseNode(node: scala.xml.Node): Either[(JobStaticParam, Extract, Transform, Load), String] ={
    val trimmedNode = scala.xml.Utility.trim(node)
    val etlJob = trimmedNode match {
      case <etlJob>{ children @ _* }</etlJob> => ETLJob.fromXML(trimmedNode)
      case _ => return Right(s"Unknown entity found instead of '<etlJob>'")
    }
    Left((etlJob.jobStaticParam, etlJob.extract, etlJob.transform, etlJob.load))
  }
}

