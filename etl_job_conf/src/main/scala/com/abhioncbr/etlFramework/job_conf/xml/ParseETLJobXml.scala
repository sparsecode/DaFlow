package com.abhioncbr.etlFramework.job_conf.xml

import java.io._

import com.abhioncbr.etlFramework.commons.ContextConstantEnum.HADOOP_CONF
import com.abhioncbr.etlFramework.commons.extract.Extract
import com.abhioncbr.etlFramework.commons.Context
import com.abhioncbr.etlFramework.commons.job.{ETLJob, JobStaticParam}
import com.abhioncbr.etlFramework.commons.load.Load
import com.abhioncbr.etlFramework.commons.transform.Transform
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object ETLJob{
  def fromXML(node: scala.xml.NodeSeq): ETLJob = {
      new ETLJob(ParseJobStaticParam.fromXML(node \ "job_static_param"),
        ParseExtractConf.fromXML(node \ "extract"),
        ParseTransform.fromXML(node \ "transform"),
        ParseLoad.fromXML(node \ "load")
    )
  }
}

class ParseETLJobXml {

  def parseXml(path: String, loadFromHdfs: Boolean): Either[String, String] = {
    try {
      var reader: BufferedReader = null

      if(loadFromHdfs) {
        val fs = FileSystem.get(Context.getContextualObject[Configuration](HADOOP_CONF))
        reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      } else {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
      }

      val lines = Stream.continually(reader.readLine()).takeWhile(_ != null).toArray[String].mkString
      reader.close()
      Left(lines)
    } catch {
      case e: FileNotFoundException => Right("Not able to load job xml file. Provided path: " + path)
      case ex: Exception => Right("Exception while parsing job xml file. Please validate xml.")
    }
  }

  def parseNode(node: scala.xml.Node): Either[(JobStaticParam, Extract, Transform, Load), String] ={
    val trimmedNode = scala.xml.Utility.trim(node)
    val etlJob = trimmedNode match {
      case <etl_job>{ children @ _* }</etl_job> => ETLJob.fromXML(trimmedNode)
      case _ => return Right(s"Unknown entity found instead of '<etl_job>'")
    }
    Left((etlJob.jobStaticParam, etlJob.extract, etlJob.transform, etlJob.load))
  }
}

